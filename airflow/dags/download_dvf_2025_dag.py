from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from utils.notifications import notify_failure
import requests
import os
import zipfile
import csv
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


# Chemin où seront stockés les fichiers sur le volume Docker
DATA_DIR = Path("/opt/airflow/output")

# Paramètres DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,  # Retry si échec
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure
}

# Requête SQL pour transformer bronze -> silver
TRANSFORM_DVF_SQL = """
DROP TABLE IF EXISTS silver.dvf;

CREATE TABLE silver.dvf AS
SELECT

    "Identifiant de document"::TEXT AS document_id,
    COALESCE("Reference document", '')::TEXT AS reference_document,

    COALESCE(NULLIF("No disposition", ''), '0')::INT AS disposition_id,

    COALESCE(
        "Date mutation"::DATE,
        DATE '1900-01-01'
    ) AS date_mutation,

    COALESCE("Nature mutation", '')::TEXT AS nature_mutation,

    COALESCE(
        REPLACE(NULLIF("Valeur fonciere", ''), ',', '.')::NUMERIC,
        0
    ) AS valeur_fonciere,

    COALESCE(NULLIF("No voie", ''), '0')::INT AS numero_voie,

    COALESCE("Type de voie", '')::TEXT AS type_voie,

    COALESCE("Voie", '')::TEXT AS voie,

    COALESCE("Code postal", '')::TEXT AS code_postal,

    COALESCE("Commune", '')::TEXT AS commune,

    COALESCE("Code departement", '')::TEXT AS departement,

    COALESCE("Code commune", '')::INT AS code_commune,

    COALESCE("Type local", '')::TEXT AS type_local,

    COALESCE(
        NULLIF("Surface reelle bati", '')::FLOAT,
        0
    ) AS surface_bati,

    COALESCE(
        NULLIF("Nombre pieces principales", '')::INT,
        0
    ) AS nb_pieces,

    COALESCE(
        NULLIF("Surface terrain", '')::FLOAT,
        0
    ) AS surface_terrain

FROM bronze.dvf_data;
"""


@dag(
    dag_id="dvf_2025_extraction",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@monthly",  # ajustable
    catchup=False,
    tags=["extraction", "DVF"],
)
def dvf_2025_dag():

    @task()
    def download_and_extract_dvf():
        # URL du fichier DVF 2025 (exemple, à remplacer par l’URL exacte)
        url = (
            "https://www.data.gouv.fr/api/1/"
            "datasets/r/4d741143-8331-4b59-95c2-3b24a7bdbe3c"
        )

        # Crée le dossier si n’existe pas
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        zip_path = os.path.join(DATA_DIR, "dvf_2025.zip")

        # téléchargement
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"Erreur téléch. DVF : {response.status_code}")

        with open(zip_path, "wb") as f:
            f.write(response.content)

        # décompression
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(DATA_DIR)

        # conversion TXT -> CSV
        txt_file = [f for f in os.listdir(DATA_DIR) if f.endswith(".txt")][0]

        txt_path = os.path.join(DATA_DIR, txt_file)
        csv_path = os.path.join(DATA_DIR, "dvf_2025.csv")

        with open(txt_path, "r", encoding="latin-1") as txt_f, \
             open(csv_path, "w", newline="", encoding="utf-8") as csv_f:

            reader = csv.reader(txt_f, delimiter="|")
            writer = csv.writer(csv_f)

            for row in reader:
                writer.writerow(row)

        os.remove(zip_path)
        os.remove(txt_path)

        return f"CSV généré : {csv_path}"

    def load_to_bronze(**context):
        """Lecture du JSON → insertion dans bronze.meteo_quotidien."""
        src = DATA_DIR / "dvf_2025.csv"
        if not src.exists():
            raise FileNotFoundError(f"Fichier non trouvé : {src}")

        df = pd.read_csv(
            src,
            sep=",",
            dtype=str,
            low_memory=False
        )

        # Select and rename useful columns
        df = df[[
            "Identifiant de document",
            "Reference document",
            "No disposition",
            "Date mutation",
            "Nature mutation",
            "Valeur fonciere",
            "No voie",
            "Type de voie",
            "Voie",
            "Code postal",
            "Commune",
            "Code departement",
            "Code commune",
            "Type local",
            "Surface reelle bati",
            "Nombre pieces principales",
            "Surface terrain"
        ]]

        # Optional cleaning (keep bronze mostly raw but fix encoding issues)
        df["Valeur fonciere"] = df["Valeur fonciere"].str.replace(",", ".")
        df["Date mutation"] = pd.to_datetime(
                                 df["Date mutation"],
                                 format="%d/%m/%Y",
                                 errors="coerce"
                              )

        engine = create_engine(
            "postgresql://svc_dwh:svc_dwh@postgres:5432/warehouse"
        )

        df.to_sql(
            "dvf_data",
            engine,
            schema="bronze",
            if_exists="replace",
            index=False,
        )

    # Tâches
    task_fetch = download_and_extract_dvf()

    load_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="postgres_warehouse",
        sql=TRANSFORM_DVF_SQL,
    )

    # Chaînage
    task_fetch >> load_bronze >> transform_silver


# Instanciation du DAG
dvf_2025_dag = dvf_2025_dag()

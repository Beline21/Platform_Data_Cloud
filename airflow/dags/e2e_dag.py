from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

import requests
import json
import os
import zipfile
import csv
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

from utils.notifications import notify_failure

# ======================
# CONFIG
# ======================
DATA_DIR = Path("/opt/airflow/output")
DBT_PROJECT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure
}

# ======================
# METEO (copié tel quel sans @task)
# ======================
def fetch_meteo():
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=52.52&longitude=13.41&hourly=temperature_2m"
    )

    filename = DATA_DIR / "open_meteo_berlin.json"

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        with open(filename, "w") as f:
            json.dump(data, f, indent=4)

        return f"Météo Berlin téléchargée : {filename}"
    else:
        raise Exception(f"Erreur API Open-Meteo : {response.status_code}")


def load_meteo_to_bronze(**context):
    src = DATA_DIR / "open_meteo_berlin.json"
    if not src.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {src}")

    with open(src) as f:
        data = json.load(f)

    hourly = data["hourly"]

    df = pd.DataFrame({
        "time": hourly["time"],
        "temperature_2m": hourly["temperature_2m"]
    })

    df["latitude"] = data["latitude"]
    df["longitude"] = data["longitude"]
    df["elevation"] = data["elevation"]
    df["generationtime_ms"] = data["generationtime_ms"]
    df["utc_offset_seconds"] = data["utc_offset_seconds"]
    df["timezone"] = data["timezone"]
    df["timezone_abbreviation"] = data["timezone_abbreviation"]

    engine = create_engine(
        "postgresql://svc_dwh:svc_dwh@postgres:5432/warehouse"
    )

    df.to_sql(
        "meteo_quotidien",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
    )


# ======================
# DVF (copié tel quel sans @task)
# ======================
def fetch_dvf():
    url = (
        "https://www.data.gouv.fr/api/1/"
        "datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    zip_path = os.path.join(DATA_DIR, "dvf_2025.zip")

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Erreur téléch. DVF : {response.status_code}")

    with open(zip_path, "wb") as f:
        f.write(response.content)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(DATA_DIR)

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


def load_dvf_to_bronze(**context):
    src = DATA_DIR / "dvf_2025.csv"
    if not src.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {src}")

    df = pd.read_csv(
        src,
        sep=",",
        dtype=str,
        low_memory=False
    )

    df = df[[
        "No disposition",
        "Date mutation",
        "Nature mutation",
        "Valeur fonciere",
        "No voie",
        "Type de voie",
        "Code voie",
        "Voie",
        "Code postal",
        "Commune",
        "Code departement",
        "Code commune",
        "Section",
        "No plan",
        "Code type local",
        "Type local",
        "Surface reelle bati",
        "Nombre pieces principales",
        "Nature culture",
        "Surface terrain"
    ]]

    engine = create_engine(
        "postgresql://svc_dwh:svc_dwh@postgres:5432/warehouse"
    )

    df.to_sql(
        "dvf_mutations",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
    )


# ======================
# DAG
# ======================
with DAG(
    dag_id="elt_e2e",
    start_date=datetime(2026, 3, 1),
    default_args=default_args,
    tags=["e2e", "elt"],
) as dag:

    extract_meteo = PythonOperator(
        task_id="e2e_extract_meteo",
        python_callable=fetch_meteo,
    )

    extract_dvf = PythonOperator(
        task_id="e2e_extract_dvf",
        python_callable=fetch_dvf,
    )

    load_meteo = PythonOperator(
        task_id="e2e_load_meteo_bronze",
        python_callable=load_meteo_to_bronze,
    )

    load_dvf = PythonOperator(
        task_id="e2e_load_dvf_bronze",
        python_callable=load_dvf_to_bronze,
    )

    run_dbt = BashOperator(
        task_id="e2e_run_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        cwd=DBT_PROJECT_DIR,
    )

    dbt_test = BashOperator(
        task_id="e2e_dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        cwd=DBT_PROJECT_DIR,
    )

    chain(
        [extract_meteo, extract_dvf],
        [load_meteo, load_dvf],
        run_dbt,
        dbt_test
    )

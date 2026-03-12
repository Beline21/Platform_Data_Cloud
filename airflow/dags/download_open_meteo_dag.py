from airflow.sdk import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from utils.notifications import notify_failure
import requests
import os
import json
import pandas as pd
from sqlalchemy import create_engine


# Volume Docker pour persistance
DATA_DIR = "/opt/airflow/output"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,             # retry automatique en cas d'échec
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure
}

# Requête SQL pour transformer bronze -> silver
TRANSFORM_METEO_SQL = """
DROP TABLE IF EXISTS silver.meteo_quotidien;
CREATE TABLE silver.meteo_quotidien AS
SELECT
    col1::type1 AS col1,
    COALESCE(col2::type2, 0) AS col2,
    COALESCE(col3::type3, 0) AS col3,
    COALESCE(col4::type4, 0) AS col4
FROM bronze.meteo_quotidien;
"""


@dag(
    dag_id="open_meteo_berlin",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@monthly",  # toutes les heures pour ce DAG
    catchup=False,
    tags=["extraction", "meteo"],
)
def open_meteo_berlin_dag():

    @task()
    def fetch_weather():
        # URL Open-Meteo pour Berlin (température horaire)
        url = (
            "https://api.open-meteo.com/v1/forecast?"
            "latitude=52.52&longitude=13.41&hourly=temperature_2m"
        )
        filename = os.path.join(DATA_DIR, "open_meteo_berlin.json")

        # Créer le dossier si nécessaire
        os.makedirs(DATA_DIR, exist_ok=True)

        # Requête HTTP
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            # Sauvegarde du JSON dans le volume Docker
            with open(filename, "w") as f:
                json.dump(data, f, indent=4)

            return f"Météo Berlin téléchargée : {filename}"
        else:
            raise Exception(f"Erreur API Open-Meteo : {response.status_code}")

    def load_to_bronze(**context):
        """Lecture du JSON → insertion dans bronze.meteo_quotidien."""
        src = DATA_DIR / "<mon_fichier>.json"
        if not src.exists():
            raise FileNotFoundError(f"Fichier non trouvé : {src}")

        with open(filename) as f:
            data = json.load(f)

        # Exemple : adapter selon la structure du JSON Open-Meteo
        df = pd.DataFrame({
            "col1": data["col1"],
            "col2": data["col2"],
            "col3": data["col3"],
            "col4": data["col4"],
        })

        engine = create_engine("postgresql://svc_dwh:svc_dwh@postgres:5432/warehouse")
        df.to_sql("meteo_quotidien", engine, schema="bronze", if_exists="append", index=False)

    # Tâches
    task_fetch = fetch_weather()
    
    load_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="postgres_warehouse",
        sql=TRANSFORM_METEO_SQL,
    )

    # Chaînage
    task_fetch >> load_bronze >> transform_silver


# Instanciation du DAG
open_meteo_berlin_dag = open_meteo_berlin_dag()

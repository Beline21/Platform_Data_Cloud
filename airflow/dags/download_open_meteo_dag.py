from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from utils.notifications import notify_failure
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


# Volume Docker pour persistance
DATA_DIR = Path("/opt/airflow/output")

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
    TO_TIMESTAMP(time, 'YYYY-MM-DD"T"HH24:MI') AS time,
    COALESCE(temperature_2m::FLOAT, 0) AS temperature_2m,
    COALESCE(latitude::FLOAT, 0) AS latitude,
    COALESCE(longitude::FLOAT, 0) AS longitude,
    COALESCE(elevation::FLOAT, 0) AS elevation,
    COALESCE(generationtime_ms::FLOAT, 0) AS generationtime_ms,
    COALESCE(timezone::TEXT, '') AS timezone
FROM bronze.meteo_quotidien;

ALTER TABLE silver.meteo_quotidien
ADD COLUMN cle_primaire BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;
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
        filename = DATA_DIR / "open_meteo_berlin.json"

        # Créer le dossier si nécessaire
        DATA_DIR.mkdir(parents=True, exist_ok=True)

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
        src = DATA_DIR / "open_meteo_berlin.json"
        if not src.exists():
            raise FileNotFoundError(f"Fichier non trouvé : {src}")

        with open(src) as f:
            data = json.load(f)

        # Flatten hourly data
        hourly = data["hourly"]
        df = pd.DataFrame({
            "time": hourly["time"],
            "temperature_2m": hourly["temperature_2m"]
        })

        # Ajouter les colonnes globales
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
if __name__ == "__main__":
    open_meteo_berlin_dag = open_meteo_berlin_dag()

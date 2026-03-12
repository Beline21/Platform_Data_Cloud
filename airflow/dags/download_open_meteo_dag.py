from airflow.sdk import dag, task
from datetime import datetime, timedelta
from utils.notifications import notify_failure
import requests
import os
import json

# Volume Docker pour persistance
DATA_DIR = "/opt/airflow/output"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,             # retry automatique en cas d'échec
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure
}


@dag(
    dag_id="open_meteo_berlin",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",  # toutes les heures pour ce DAG
    catchup=False,
    tags=["extraction", "meteo"],
)
def open_meteo_berlin_dag():

    @task()
    def fetch_weather():
        # URL Open-Meteo pour Berlin (température horaire)
        url = (
            "https://api.open-meteo.com/v1/forecast"
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

    fetch_weather()


# Instanciation du DAG
open_meteo_berlin_dag = open_meteo_berlin_dag()

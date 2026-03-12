from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import os

# Chemin où seront stockés les fichiers sur le volume Docker
DATA_DIR = "/opt/airflow/output"

# Paramètres DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,  # Retry si échec
    "retry_delay": timedelta(minutes=5),
}


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
    def download_dvf():
        # URL du fichier DVF 2025 (exemple, à remplacer par l’URL exacte)
        url = (
            "https://www.data.gouv.fr/api/1/"
            "datasets/r/4d741143-8331-4b59-95c2-3b24a7bdbe3c"
        )
        filename = os.path.join(DATA_DIR, "dvf_2025.csv")

        # Crée le dossier si n’existe pas
        os.makedirs(DATA_DIR, exist_ok=True)

        # Téléchargement
        response = requests.get(url)
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
            return f"Fichier téléchargé avec succès : {filename}"
        else:
            raise Exception(f"Erreur télécharg. DVF : {response.status_code}")

    download_dvf()


# Instanciation du DAG
dvf_2025_dag = dvf_2025_dag()

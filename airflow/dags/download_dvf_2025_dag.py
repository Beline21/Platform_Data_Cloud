from airflow.sdk import dag, task
from datetime import datetime, timedelta
from utils.notifications import notify_failure
import requests
import os
import zipfile
import csv

# Chemin où seront stockés les fichiers sur le volume Docker
DATA_DIR = "/opt/airflow/output"

# Paramètres DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,  # Retry si échec
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure
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
    def download_and_extract_dvf():
        # URL du fichier DVF 2025 (exemple, à remplacer par l’URL exacte)
        url = (
            "https://www.data.gouv.fr/api/1/"
            "datasets/r/4d741143-8331-4b59-95c2-3b24a7bdbe3c"
        )

        # Crée le dossier si n’existe pas
        os.makedirs(DATA_DIR, exist_ok=True)

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

    download_and_extract_dvf()


# Instanciation du DAG
dvf_2025_dag = dvf_2025_dag()

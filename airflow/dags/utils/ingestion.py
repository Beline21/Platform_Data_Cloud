import csv
import json
import os
import zipfile
from datetime import datetime
from pathlib import Path

import requests
import snowflake.connector
from airflow.hooks.base import BaseHook
from airflow.models import Variable


DATA_DIR = Path("/opt/airflow/output")


# ======================
# METEO
# ======================

def fetch_meteo():
    """Télécharge les données météo Open-Meteo."""
    
    url = Variable.get("METEO_URL")

    filename = DATA_DIR / "open_meteo_berlin.json"

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(
            f"Erreur API Open-Meteo : {response.status_code}"
        )

    data = response.json()

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=4,
        )

    return f"Météo téléchargée : {filename}"


# ======================
# DVF
# ======================

def fetch_dvf():
    """Télécharge le fichier DVF et le convertit en CSV."""

    url = Variable.get("DVF_URL")

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    zip_path = DATA_DIR / "dvf_2025.zip"

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(
            f"Erreur téléchargement DVF : {response.status_code}"
        )

    with open(zip_path, "wb") as file:
        file.write(response.content)

    with zipfile.ZipFile(zip_path) as zip_file:
        zip_file.extractall(DATA_DIR)

    txt_file = next(
        file_name
        for file_name in os.listdir(DATA_DIR)
        if file_name.endswith(".txt")
    )

    txt_path = DATA_DIR / txt_file
    csv_path = DATA_DIR / "dvf_2025.csv"

    with open(
        txt_path,
        "r",
        encoding="latin-1",
    ) as txt_file, open(
        csv_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        reader = csv.reader(
            txt_file,
            delimiter="|",
        )

        writer = csv.writer(csv_file)

        for row in reader:
            writer.writerow(row)

    os.remove(zip_path)
    os.remove(txt_path)

    return f"CSV généré : {csv_path}"


# ======================
# SNOWFLAKE STAGE
# ======================

def get_snowflake_connection():
    """Création de la connexion Snowflake."""

    conn_info = BaseHook.get_connection(
        "snowflake_platform",
    )

    extra = conn_info.extra_dejson

    return snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get(
            "role",
            "ACCOUNTADMIN",
        ),
    )


def put_dvf_to_raw_stage():
    """Dépose le CSV DVF dans le raw stage Snowflake."""

    cnx = get_snowflake_connection()

    local_path = DATA_DIR / "dvf_2025.csv"

    cursor = cnx.cursor()

    cursor.execute(
        f"""
        PUT file://{local_path}
        @PLATFORM_DB.BRONZE.RAW_STAGE/dvf/annee=2025/
        AUTO_COMPRESS=FALSE
        OVERWRITE=TRUE
        """
    )

    cursor.close()
    cnx.close()


def put_meteo_to_raw_stage():
    """Dépose le JSON météo dans le raw stage Snowflake."""

    cnx = get_snowflake_connection()

    today = datetime.now().strftime("%Y-%m-%d")

    local_path = DATA_DIR / "open_meteo_berlin.json"

    cursor = cnx.cursor()

    cursor.execute(
        f"""
        PUT file://{local_path}
        @PLATFORM_DB.BRONZE.RAW_STAGE/meteo/date={today}/
        AUTO_COMPRESS=FALSE
        OVERWRITE=TRUE
        """
    )

    cursor.close()
    cnx.close()

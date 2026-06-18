from airflow.models import Variable
from airflow.hooks.base import BaseHook

from pathlib import Path
from datetime import datetime

import requests
import json
import zipfile
import csv
import os


DATA_DIR = Path("/opt/airflow/output")


# ======================
# METEO
# ======================

def fetch_meteo():

    url = Variable.get("METEO_URL")

    filename = DATA_DIR / "open_meteo_berlin.json"

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(
            f"Erreur API Open-Meteo : {response.status_code}"
        )

    data = response.json()

    with open(filename, "w") as f:
        json.dump(
            data,
            f,
            indent=4
        )

    return f"Météo téléchargée : {filename}"


# ======================
# DVF
# ======================

def fetch_dvf():

    url = Variable.get("DVF_URL")

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    zip_path = DATA_DIR / "dvf_2025.zip"

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(
            f"Erreur téléchargement DVF : {response.status_code}"
        )

    with open(zip_path, "wb") as f:
        f.write(response.content)

    with zipfile.ZipFile(zip_path) as z:
        z.extractall(DATA_DIR)

    txt_file = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".txt")
    ][0]

    txt_path = DATA_DIR / txt_file

    csv_path = DATA_DIR / "dvf_2025.csv"

    with open(
        txt_path,
        "r",
        encoding="latin-1"
    ) as txt_f, \
    open(
        csv_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as csv_f:

        reader = csv.reader(
            txt_f,
            delimiter="|"
        )

        writer = csv.writer(
            csv_f
        )

        for row in reader:
            writer.writerow(row)

    os.remove(zip_path)
    os.remove(txt_path)

    return f"CSV généré : {csv_path}"


# ======================
# SNOWFLAKE STAGE
# ======================

def put_dvf_to_raw_stage():

    import snowflake.connector

    conn_info = BaseHook.get_connection(
        "snowflake_platform"
    )

    extra = conn_info.extra_dejson

    cnx = snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get(
            "role",
            "ACCOUNTADMIN"
        ),
    )

    local_path = (
        DATA_DIR /
        "dvf_2025.csv"
    )

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

    import snowflake.connector

    conn_info = BaseHook.get_connection(
        "snowflake_platform"
    )

    extra = conn_info.extra_dejson

    cnx = snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get(
            "role",
            "ACCOUNTADMIN"
        ),
    )

    today = datetime.now().strftime(
        "%Y-%m-%d"
    )

    local_path = (
        DATA_DIR /
        "open_meteo_berlin.json"
    )

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

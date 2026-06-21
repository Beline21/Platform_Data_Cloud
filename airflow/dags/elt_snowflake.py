import csv
import json
import os
import zipfile

import requests

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.notifications import notify_failure
from airflow.models.baseoperator import chain


# ======================
# CONFIG
# ======================


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DATA_DIR = Path("/opt/airflow/output")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure
}


# ======================
# Functions
# ======================


def fetch_meteo():
    url = Variable.get("METEO_URL")

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


def fetch_dvf():
    url = Variable.get("DVF_URL")

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


def put_dvf_to_raw_stage(**context):
    import snowflake.connector
    from airflow.hooks.base import BaseHook
    from pathlib import Path
    import os

    conn_info = BaseHook.get_connection("snowflake_platform")
    extra = conn_info.extra_dejson

    cnx = snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get("role", "ACCOUNTADMIN"),
    )

    local_path = (
        Path(os.environ.get("DATA_DIR", "/opt/airflow/output"))
        / "dvf_2025.csv"
    )
    if not local_path.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {local_path}")

    cursor = cnx.cursor()
    # PUT dépose le fichier dans le stage avec un préfixe partitionné
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


def put_meteo_to_raw_stage(**context):
    import snowflake.connector
    from airflow.hooks.base import BaseHook
    from pathlib import Path
    from datetime import datetime
    import os

    conn_info = BaseHook.get_connection("snowflake_platform")
    extra = conn_info.extra_dejson

    cnx = snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get("role", "ACCOUNTADMIN"),
    )

    today = datetime.now().strftime("%Y-%m-%d")
    local_path = (
        Path(os.environ.get("DATA_DIR", "/opt/airflow/output"))
        / "open_meteo_berlin.json"
    )
    if not local_path.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {local_path}")

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


# ======================
# DAG
# ======================


with DAG(
    dag_id="elt_snowflake",
    schedule=None,
    start_date=datetime(2026, 3, 1),
    default_args=default_args,
    tags=["snowflake", "elt"],
) as dag:

    # E : extraction (réutiliser les fonctions existantes)
    extract_meteo = PythonOperator(
        task_id="extract_meteo",
        python_callable=fetch_meteo
    )
    extract_dvf = PythonOperator(
        task_id="extract_dvf",
        python_callable=fetch_dvf
    )

    # L : PUT dans raw stage
    put_meteo = PythonOperator(
        task_id="put_meteo_raw",
        python_callable=put_meteo_to_raw_stage
    )
    put_dvf = PythonOperator(
        task_id="put_dvf_raw",
        python_callable=put_dvf_to_raw_stage
    )

    # L : CREATE table into bronze (if not exists)
    create_dvf_bronze = SQLExecuteQueryOperator(
        task_id="create_dvf_bronze_table",
        conn_id="snowflake_platform",
        sql="""
            CREATE TABLE IF NOT EXISTS PLATFORM_DB.BRONZE.DVF_MUTATIONS (
                "Identifiant de document"    VARCHAR,
                "Reference document"         VARCHAR,
                "1 Articles CGI"             VARCHAR,
                "2 Articles CGI"             VARCHAR,
                "3 Articles CGI"             VARCHAR,
                "4 Articles CGI"             VARCHAR,
                "5 Articles CGI"             VARCHAR,
                "No disposition"             INTEGER,
                "Date mutation"              DATE,
                "Nature mutation"            VARCHAR,
                "Valeur fonciere"            VARCHAR,
                "No voie"                    INTEGER,
                "B/T/Q"                      VARCHAR,
                "Type de voie"               VARCHAR,
                "Code voie"                  VARCHAR,
                "Voie"                       VARCHAR,
                "Code postal"                VARCHAR,
                "Commune"                    VARCHAR,
                "Code departement"           VARCHAR,
                "Code commune"               INTEGER,
                "Prefixe de section"         VARCHAR,
                "Section"                    VARCHAR,
                "No plan"                    INTEGER,
                "No Volume"                  VARCHAR,
                "1er lot"                    VARCHAR,
                "Surface Carrez du 1er lot"  VARCHAR,
                "2eme lot"                   VARCHAR,
                "Surface Carrez du 2eme lot" VARCHAR,
                "3eme lot"                   VARCHAR,
                "Surface Carrez du 3eme lot" VARCHAR,
                "4eme lot"                   VARCHAR,
                "Surface Carrez du 4eme lot" VARCHAR,
                "5eme lot"                   VARCHAR,
                "Surface Carrez du 5eme lot" VARCHAR,
                "Nombre de lots"             VARCHAR,
                "Code type local"            VARCHAR,
                "Type local"                 VARCHAR,
                "Identifiant local"          VARCHAR,
                "Surface reelle bati"        DOUBLE,
                "Nombre pieces principales"  INTEGER,
                "Nature culture"             VARCHAR,
                "Nature culture speciale"    VARCHAR,
                "Surface terrain"            DOUBLE
                )
            """,
        )
    create_meteo_bronze = SQLExecuteQueryOperator(
        task_id="create_meteo_bronze_table",
        conn_id="snowflake_platform",
        sql="""
            CREATE TABLE IF NOT EXISTS PLATFORM_DB.BRONZE.METEO_QUOTIDIEN (
                latitude              DOUBLE,
                longitude             DOUBLE,
                generationtime_ms     DOUBLE,
                utc_offset_seconds    BIGINT,
                timezone              VARCHAR,
                timezone_abbreviation VARCHAR,
                elevation             DOUBLE,
                hourly_units          VARIANT,
                hourly                VARIANT
                )
            """,
        )

    # L : COPY INTO bronze
    copy_dvf = SQLExecuteQueryOperator(
        task_id="copy_dvf_bronze",
        conn_id="snowflake_platform",
        sql="""
            COPY INTO PLATFORM_DB.BRONZE.DVF_MUTATIONS
            FROM @PLATFORM_DB.BRONZE.RAW_STAGE/dvf/annee=2025/
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = ','
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL')
            )
            ON_ERROR = 'CONTINUE'
        """,
    )
    copy_meteo = SQLExecuteQueryOperator(
        task_id="copy_meteo_bronze",
        conn_id="snowflake_platform",
        sql="""
            COPY INTO PLATFORM_DB.BRONZE.METEO_QUOTIDIEN
            (
                latitude,
                longitude,
                generationtime_ms,
                utc_offset_seconds,
                timezone,
                timezone_abbreviation,
                elevation,
                hourly_units,
                hourly
            )
            FROM (
                SELECT
                    $1:latitude::DOUBLE,
                    $1:longitude::DOUBLE,
                    $1:generationtime_ms::DOUBLE,
                    $1:utc_offset_seconds::BIGINT,
                    $1:timezone::VARCHAR,
                    $1:timezone_abbreviation::VARCHAR,
                    $1:elevation::DOUBLE,
                    $1:hourly_units,
                    $1:hourly
                FROM @PLATFORM_DB.BRONZE.RAW_STAGE/meteo/date={{ ds }}/
            )
            FILE_FORMAT = (
                TYPE = JSON
            )
            ON_ERROR = 'CONTINUE'
        """,
    )

    # T : dbt run --target snowflake
    run_dbt = BashOperator(
        task_id="run_dbt_snowflake",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --target snowflake",
    )

    chain(
        [extract_meteo, extract_dvf],
        [put_meteo, put_dvf],
        [create_dvf_bronze, create_meteo_bronze],
        [copy_dvf, copy_meteo],
        [run_dbt]
    )

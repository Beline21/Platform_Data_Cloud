from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
from airflow.hooks.base import BaseHook
from airflow.models import Variable

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
# METEO
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

    conn = BaseHook.get_connection("postgres_warehouse")
    engine = create_engine(
        (
            f"postgresql://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
    )

    df.to_sql(
        "meteo_quotidien",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
    )


# ======================
# DVF
# ======================


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

    conn = BaseHook.get_connection("postgres_warehouse")
    engine = create_engine(
        (
            f"postgresql://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
    )

    df.to_sql(
        "dvf_mutations",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
    )


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

    dvf_to_raw_stage = PythonOperator(
        task_id="e2e_dvf_to_raw_stage",
        python_callable=put_dvf_to_raw_stage,
    )

    meteo_to_raw_stage = PythonOperator(
        task_id="e2e_meteo_to_raw_stage",
        python_callable=put_meteo_to_raw_stage,
    )

    # Créer la table bronze DVF (si elle n'existe pas)
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
                "Valeur fonciere"            NUMBER,
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

    # COPY INTO depuis le stage (format CSV pipe)
    copy_dvf_bronze = SQLExecuteQueryOperator(
        task_id="copy_dvf_to_bronze",
        conn_id="snowflake_platform",
        sql="""
            COPY INTO PLATFORM_DB.BRONZE.DVF_MUTATIONS
            FROM @PLATFORM_DB.BRONZE.RAW_STAGE/dvf/annee=2025/
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL')
            )
            ON_ERROR = 'CONTINUE'
        """,
    )

    # Créer la table bronze Meteo (si elle n'existe pas)
    create_meteo_bronze = SQLExecuteQueryOperator(
        task_id="create_meteo_bronze_table",
        conn_id="snowflake_platform",
        sql="""
            CREATE TABLE IF NOT EXISTS PLATFORM_DB.BRONZE.METEO (
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

    # COPY INTO depuis le stage (format JSON pipe)
    copy_meteo_bronze = SQLExecuteQueryOperator(
        task_id="copy_meteo_to_bronze",
        conn_id="snowflake_platform",
        sql="""
            COPY INTO PLATFORM_DB.BRONZE.METEO
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
                FROM @PLATFORM_DB.BRONZE.RAW_STAGE/meteo/date=2026-06-16/
            )
            FILE_FORMAT = (
                TYPE = JSON
            )
            ON_ERROR = 'CONTINUE'
        """,
    )

    chain(
        [extract_meteo, extract_dvf],
        [load_meteo, load_dvf],
        run_dbt,
        dbt_test,
        [dvf_to_raw_stage, meteo_to_raw_stage],
        [create_dvf_bronze, create_meteo_bronze],
        [copy_dvf_bronze, copy_meteo_bronze]
    )

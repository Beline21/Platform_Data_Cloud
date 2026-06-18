from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from e2e_dag import (
    fetch_meteo,
    fetch_dvf,
    put_meteo_to_raw_stage,
    put_dvf_to_raw_stage,
)

DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="elt_snowflake",
    schedule=None,
    start_date=days_ago(1),
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

    (
        [extract_meteo, extract_dvf]
        >> [put_meteo, put_dvf]
        >> [create_dvf_bronze, create_meteo_bronze]
        >> [copy_dvf, copy_meteo]
        >> run_dbt
    )

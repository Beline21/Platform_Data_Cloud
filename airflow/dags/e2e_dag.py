from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models.baseoperator import chain

# Importer vos fonctions existantes
from download_open_meteo_dag import fetch_weather as fetch_meteo
from download_open_meteo_dag import (
    load_to_bronze as load_meteo_to_bronze
)
from download_dvf_2025_dag import (
    download_and_extract_dvf as fetch_dvf
)
from download_dvf_2025_dag import (
    load_to_bronze as load_dvf_to_bronze
)
# from load_bronze import load_meteo_to_bronze, load_dvf_to_bronze

DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="elt_e2e",
    start_date=datetime(2026, 3, 1),
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

    chain(
        [extract_meteo, extract_dvf]
        >> [load_meteo, load_dvf]
        >> run_dbt
        >> dbt_test
    )

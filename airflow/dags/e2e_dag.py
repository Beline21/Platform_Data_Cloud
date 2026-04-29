from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Importer vos fonctions existantes
from download_open_meteo_dag import fetch_weather as fetch_meteo
from download_open_meteo_dag import load_to_bronze as load_meteo_to_bronze
from download_dvf_2025_dag import download_and_extract_dvf as fetch_dvf
from download_dvf_2025_dag import load_to_bronze as load_dvf_to_bronze
# from load_bronze import load_meteo_to_bronze, load_dvf_to_bronze

DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="elt_e2e",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["e2e", "elt"],
) as dag:

    extract_meteo = PythonOperator(
        task_id="extract_meteo",
        python_callable=fetch_meteo,
    )
    extract_dvf = PythonOperator(
        task_id="extract_dvf",
        python_callable=fetch_dvf,
    )
    load_meteo = PythonOperator(
        task_id="load_meteo_bronze",
        python_callable=load_meteo_to_bronze,
    )
    load_dvf = PythonOperator(
        task_id="load_dvf_bronze",
        python_callable=load_dvf_to_bronze,
    )
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        cwd=DBT_PROJECT_DIR,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        cwd=DBT_PROJECT_DIR,
    )

    [extract_meteo, extract_dvf] >> [load_meteo, load_dvf] >> run_dbt >> dbt_test

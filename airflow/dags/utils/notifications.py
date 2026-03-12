import requests
from airflow.models import TaskInstance


def notify_failure(context: dict):
    """
    Fonction appelée par Airflow lorsqu'une tâche échoue.
    context contient des infos sur le DAG, task, run.
    """
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    run_id = context.get('run_id')
    ts = context.get('ts')  # horodatage Airflow
    state = context.get('task_instance').state

    message = (
        f":x: DAG `{dag_id}`, task `{task_id}` a échoué!"
        f"\nRun: {run_id}\nTime: {ts}\nStatus: {state}"
    )

    webhook_url = "https://ntfy.sh/tp-airflow"

    try:
        requests.post(webhook_url, json={"text": message})
    except Exception as e:
        print(f"Erreur lors de l'envoi de la notification: {e}")

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from train_model import train_from_env


with DAG(
    dag_id="retrain_fraud_model",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 5, 12, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["fraud", "spark", "mongo"],
) as dag:
    # Un seul job: relancer l'entraînement du modèle toutes les 5 minutes.
    PythonOperator(
        task_id="retrain_model_from_mongo",
        python_callable=train_from_env,
    )

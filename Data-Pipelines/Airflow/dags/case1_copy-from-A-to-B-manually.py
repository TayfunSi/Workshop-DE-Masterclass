from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Damit du utils importieren kannst
sys.path.append(os.path.dirname(__file__))

from utils import case1_copy_taxi_zone_manually  # Funktion aus utils.py

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='copy-from-A-to-B',
    default_args=default_args,
    schedule_interval=None,  # Nur manuell auslösen
    catchup=False,
    description='Lädt TaxiZone.csv aus raw, bereinigt und speichert in processed'
) as dag:

    ingest_task = PythonOperator(
        task_id='process_taxi_zone',
        python_callable=case1_copy_taxi_zone_manually
    )

    ingest_task
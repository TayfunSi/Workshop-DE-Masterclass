from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(__file__))
from utils import case2_copy_taxi_zone_on_file_entry  # die bestehende Funktion

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='copy-from-A-to-B-on-file-entry',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # alle 10 Minuten
    catchup=False,
    description='Triggered automatisch, wenn TaxiZone_13062025.csv vorhanden ist'
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_taxi_zone_file',
        filepath='/opt/airflow/data/raw/TaxiZone_13062025.csv',
        poke_interval=30,        # prüft alle 30 Sekunden
        timeout=60*60,           # maximal 60 Minuten warten
        mode='reschedule'              # blockiert Task bis Datei da ist
    )

    process_file = PythonOperator(
        task_id='process_taxi_zone',
        python_callable=case2_copy_taxi_zone_on_file_entry
    )

    wait_for_file >> process_file
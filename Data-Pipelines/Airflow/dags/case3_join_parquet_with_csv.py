from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(__file__))
from utils import case3_join_taxi_with_zones

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def filter_taxi_data_dynamic(**context):
    # Parameter aus trigger conf holen
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    year = conf.get('year', 2024)
    month = conf.get('month', 1)

    input_path = "data/raw/taxi_data.parquet"
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)

    output_path = f"{output_dir}/taxi_data_{year}-{month:02d}.parquet"

    df = pd.read_parquet(input_path)
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df_filtered = df[
        (df["tpep_pickup_datetime"].dt.year == int(year)) &
        (df["tpep_pickup_datetime"].dt.month == int(month))
    ]
    df_filtered.to_parquet(output_path, index=False)
    print(f"✅ Gefiltert: {len(df_filtered)} Zeilen für {year}-{month:02d}")

with DAG(
    dag_id='case3_join_parquet_with_csv',
    default_args=default_args,
    schedule_interval=None,   # Nur manuell triggern!
    catchup=False,
    description='Manuelles Filtern und Join von Taxi-Daten nach Monat/Jahr'
) as dag:

    filter_task = PythonOperator(
        task_id='filter_taxi_data_manual',
        python_callable=filter_taxi_data_dynamic,
        provide_context=True,
    )

    join_task = PythonOperator(
        task_id='join_with_zone_data_manual',
        python_callable=case3_join_taxi_with_zones,
        provide_context=True
    )

    filter_task >> join_task
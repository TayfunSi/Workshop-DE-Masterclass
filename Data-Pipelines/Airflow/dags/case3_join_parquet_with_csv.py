from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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

def filter_january_2024():
    input_path = "data/raw/taxi_data.parquet"
    output_path = "data/processed/taxi_data_2024-01.parquet"
    os.makedirs("data/processed", exist_ok=True)

    df = pd.read_parquet(input_path)
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df_filtered = df[
        (df["tpep_pickup_datetime"].dt.year == 2024) &
        (df["tpep_pickup_datetime"].dt.month == 1)
    ]

    if df_filtered.empty:
        raise ValueError("Keine Daten gefunden für Januar 2024")

    df_filtered.to_parquet(output_path, index=False)
    print(f"✅ {len(df_filtered)} Zeilen gefiltert für Januar 2024")

with DAG(
    dag_id='case3_join_parquet_with_csv',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Filtert statisch Januar 2024 und joint mit Zone-Daten',
) as dag:

    filter_task = PythonOperator(
        task_id='filter_taxi_data_jan_2024',
        python_callable=filter_january_2024,
    )

    join_task = PythonOperator(
        task_id='join_with_zone_data_jan_2024',
        python_callable=case3_join_taxi_with_zones,
    )

    filter_task >> join_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Damit utils importiert werden kann
sys.path.append(os.path.dirname(__file__))
from utils import case3_join_taxi_with_zones  # Der Join ist ausgelagert

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 1),
    'retries': 0
}

with DAG(
    dag_id='case3_join_taxi_with_zone_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Filtert Taxi-Daten pro Monat und joint mit Zone-Daten'
) as dag:

    # Funktionen können auch im DAG definiert werden und müssen nicht ausgelagert werden
    def filter_taxi_data_dynamic(execution_date, **kwargs):
        input_path = "data/raw/taxi_data.parquet"
        output_dir = "data/processed"
        os.makedirs(output_dir, exist_ok=True)

        # 📆 Vormonat berechnen
        first_of_month = execution_date.replace(day=1)
        prev_month_end = first_of_month - timedelta(days=1)
        year = prev_month_end.year
        month = prev_month_end.month

        output_path = f"{output_dir}/taxi_data_{year}-{month:02d}.parquet"

        df = pd.read_parquet(input_path)
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df_filtered = df[
            (df["tpep_pickup_datetime"].dt.year == year) &
            (df["tpep_pickup_datetime"].dt.month == month)
        ]
        df_filtered.to_parquet(output_path, index=False)
        print(f"✅ Gefiltert: {len(df_filtered)} Zeilen für {year}-{month:02d}")

    filter_task = PythonOperator(
        task_id='filter_taxi_data_monthly',
        python_callable=filter_taxi_data_dynamic,
        provide_context=True  # <-- damit execution_date übergeben wird
    )

    join_task = PythonOperator(
        task_id='join_with_zone_data',
        python_callable=case3_join_taxi_with_zones,
        provide_context=True
    )

    filter_task >> join_task
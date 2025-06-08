from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from azure.storage.blob import BlobServiceClient
import os

# Konfiguration
GITHUB_URL = "https://raw.githubusercontent.com/TayfunSi/Workshop-DE-Masterclass/main/Data-Pipelines/Airflow/data/raw/TaxiZone.csv"
LOCAL_FILENAME = "/tmp/TaxiZone.csv"

def download_from_github():
    response = requests.get(GITHUB_URL)
    response.raise_for_status()
    with open(LOCAL_FILENAME, "wb") as f:
        f.write(response.content)

def upload_to_azure_blob():
    account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    container_name = os.environ["AZURE_CONTAINER_NAME"]

    blob_service_client = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    )

    blob_client = blob_service_client.get_blob_client(container=container_name, blob="TaxiZone.csv")
    with open(LOCAL_FILENAME, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

# DAG Definition
with DAG(
    dag_id="azure_ingest_github_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "azure"],
) as dag:

    task_download = PythonOperator(
        task_id="download_from_github",
        python_callable=download_from_github
    )

    task_upload = PythonOperator(
        task_id="upload_to_azure_blob",
        python_callable=upload_to_azure_blob
    )

    task_download >> task_upload
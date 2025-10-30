from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import boto3
import json
import os


# extraindo dados da api e salvando o json localmente

def extract_coins_data(**context):

    COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
    API_URL = os.getenv("API_URL")
    LOCAL_PATH = os.getenv("LOCAL_PATH")

    if not API_URL:
        raise ValueError("A variável de ambiente 'API_URL' não foi carregada. Verifique o .env ou docker-compose.")

    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY}
    
    TIMEOUT_SECONDS = 30 
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=TIMEOUT_SECONDS)
        
    except Timeout:
        raise Exception(f"Erro de Timeout ao acessar API: a requisição excedeu {TIMEOUT_SECONDS} segundos.")
    except RequestException as e:
        raise Exception(f"Erro na requisição à API (Geral): {e}")

    if response.status_code != 200:
        raise Exception(f"Erro ao acessar API: status {response.status_code} - {response.text}")

    data = response.json()

    with open(LOCAL_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Dados extraídos e salvos em {LOCAL_PATH}")


# upando no s3 

def upload_to_s3(**context):
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    S3_PATH = os.getenv("S3_PATH")
    LOCAL_PATH = os.getenv("LOCAL_PATH")


    session = boto3.Session() 
    s3 = session.client("s3")

    execution_date = context["ds"]
    s3_key = f"{S3_PATH}/date={execution_date}/data.json"

    s3.upload_file(LOCAL_PATH, BUCKET_NAME, s3_key)
    print(f"Arquivo enviado para s3://{BUCKET_NAME}/{s3_key}")


# defnindo dag 
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="coingecko_markets_dag",
    description="Extrai dados da CoinGecko e envia ao S3 (camada bronze).",
    start_date=datetime(2025, 10, 27),  # mude a data pra sua necessidade 
    schedule="0 8 * * 1" ,
    catchup=False,                      
    default_args=default_args,
    tags=["coingecko", "crypto", "bronze", "s3"],
) as dag:


    extract_task = PythonOperator(
        task_id="extract_coins_data",
        python_callable=extract_coins_data,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    # ordem que vai acontecer 
    extract_task >> upload_task

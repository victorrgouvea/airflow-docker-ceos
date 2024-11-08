from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json

def get_json():
    with open('candidatos_2024.json', 'r') as file:
        data = json.load(file)
    
    print(data)

# Configurações do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 22),
    # 'retries': 2,
}

dag = DAG(
    'scraper_candidatos_2024',
    default_args=default_args,
    description='Executa um spider do Scrapy para coletar dados de candidatos de 2024',
    schedule_interval='@once',
)

# Tarefa que executa o Scrapy com PythonOperator
run_scrapy_task = PythonOperator(
    task_id='run_scrapy',
    python_callable=get_json,
    dag=dag,
)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, 'opt/airflow/pipelines')

from ingestion.ingestion import executar_ingestao
from transformation.transformation import executar_transformacao
from transformation.silver_to_postgres import carregar_para_postgres


default_args = {
    "owner": "data-engeniering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="gastos_publicos_pipeline",
    default_args=default_args,
    description="Pipeline completo de gastos públicos",
    schedule_interval="0 6 * * *",   # roda todo dia às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,                    # não reprocessa datas passadas
    tags=["gastos", "producao"]
) as dag:

    task_ingestion = PythonOperator(
        task_id="ingestion",
        python_callable=executar_ingestion,
    )

    task_transformacao = PythonOperator(
        task_id="transformation",
        python_callable=executar_transformacao,
    )

    task_postgres = PythonOperator(
        task_id="silver_to_postgres",
        python_callable=carregar_para_postgres,
    )

    task_ingestion >> task_transformacao >> task_postgres
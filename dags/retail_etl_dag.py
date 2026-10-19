from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import etl_lib as etl

default_args = {"owner": "nhom-kdl", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="retail_dw_etl",
    description="ETL Online Retail II -> PostgreSQL Star Schema -> RFM/K-Means",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",          # đổi thành None nếu chỉ muốn chạy thủ công
    catchup=False,
    max_active_runs=1,          # không cho 2 lần chạy song song (cùng TRUNCATE/ghi bảng)
    default_args=default_args,
    tags=["kdl", "retail"],
) as dag:
    t_ingest = PythonOperator(task_id="ingest_raw", python_callable=etl.ingest_raw)
    t_schema = PythonOperator(task_id="create_schema", python_callable=etl.create_schema)
    t_extract = PythonOperator(task_id="extract", python_callable=etl.extract)
    t_transform = PythonOperator(task_id="transform", python_callable=etl.transform)
    t_dims = PythonOperator(task_id="load_dimensions", python_callable=etl.load_dimensions)
    t_fact = PythonOperator(task_id="load_fact_sales", python_callable=etl.load_fact)
    t_rfm = PythonOperator(task_id="rfm_kmeans", python_callable=etl.rfm_kmeans)
    t_validate = PythonOperator(task_id="validate", python_callable=etl.validate)

    t_ingest >> t_schema >> t_extract >> t_transform >> t_dims >> t_fact >> t_rfm >> t_validate

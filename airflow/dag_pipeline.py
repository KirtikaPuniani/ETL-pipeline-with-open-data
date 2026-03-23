from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os

# add project scripts folder to path
sys.path.append("/opt/airflow/scripts")

from extract import extract_data
from transform import transform_data
from convert_parquet import convert_to_parquet
from upload_gcs import upload_to_gcs
from load_bigquery import load_to_bigquery


# -------- TASK FUNCTIONS -------- #

def extract_task(**kwargs):
    df = extract_data("data/WHO-COVID-19-global-daily-data.csv")
    kwargs['ti'].xcom_push(key="dataframe", value=df.to_json())


def transform_task(**kwargs):
    import pandas as pd

    df_json = kwargs['ti'].xcom_pull(key="dataframe", task_ids="extract")
    df = pd.read_json(df_json)

    df = transform_data(df)

    kwargs['ti'].xcom_push(key="dataframe", value=df.to_json())


def parquet_task(**kwargs):
    import pandas as pd

    df_json = kwargs['ti'].xcom_pull(key="dataframe", task_ids="transform")
    df = pd.read_json(df_json)

    convert_to_parquet(df, "data/covid_data.parquet")


def upload_task():
    upload_to_gcs(
        bucket_name="etl-pipeline-bucket",
        source_file="data/covid_data.parquet",
        destination_blob="data/covid_data.parquet"
    )


def load_task():
    load_to_bigquery(
        table_id="who_covid_data.trips",
        uri="gs://etl-pipeline-bucket/data/covid_data.parquet"
    )


# -------- DAG CONFIG -------- #

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}


with DAG(
    dag_id="covid_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "bigquery"]
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    convert_parquet = PythonOperator(
        task_id="convert_parquet",
        python_callable=parquet_task
    )

    upload_gcs = PythonOperator(
        task_id="upload_gcs",
        python_callable=upload_task
    )

    load_bigquery = PythonOperator(
        task_id="load_bigquery",
        python_callable=load_task
    )

    # DAG workflow order
    extract >> transform >> convert_parquet >> upload_gcs >> load_bigquery

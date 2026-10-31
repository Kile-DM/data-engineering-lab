"""Пример ETL: генерируем данные -> MinIO (S3) -> ClickHouse.

DAG намеренно использует обычный Python (boto3 + clickhouse-connect),
чтобы показать работу с хранилищами напрямую. Spark живёт в Jupyter.
"""

from __future__ import annotations

import io
import random
import uuid
from datetime import datetime, timedelta

import boto3
import clickhouse_connect
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "adminadmin"
BUCKET = "raw"

CH_HOST = "ch1-1"
CH_PORT = 8123
CH_USER = "admin"
CH_PASSWORD = "admin"

DDL = [
    "CREATE DATABASE IF NOT EXISTS lab ON CLUSTER lab_cluster",
    """
    CREATE TABLE IF NOT EXISTS lab.events ON CLUSTER lab_cluster
    (
        id     Int64,
        name   String,
        value  Float64,
        source String
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/lab/events', '{replica}')
    ORDER BY id
    """,
    """
    CREATE TABLE IF NOT EXISTS lab.events_all ON CLUSTER lab_cluster
    AS lab.events
    ENGINE = Distributed('lab_cluster', 'lab', 'events', rand())
    """,
]


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def extract(ds: str, **_):
    rows = [
        {
            "id": random.randint(1, 10_000_000),
            "name": f"event-{uuid.uuid4().hex[:8]}",
            "value": round(random.random() * 100, 4),
            "source": "airflow",
        }
        for _ in range(100)
    ]
    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False)
    buf.seek(0)
    key = f"events/dt={ds}/events.parquet"
    s3_client().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"uploaded s3://{BUCKET}/{key} ({len(rows)} rows)")


def load(ds: str, **_):
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )
    for statement in DDL:
        client.command(statement, settings={"distributed_ddl_task_timeout": 120})

    key = f"events/dt={ds}/events.parquet"
    obj = s3_client().get_object(Bucket=BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    client.insert_df("lab.events", df)
    total = client.query("SELECT count() FROM lab.events_all").first_row[0]
    print(f"inserted {len(df)} rows, lab.events_all now has {total} rows")


default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="lab_etl_minio_to_clickhouse",
    description="MinIO -> ClickHouse demo",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lab", "minio", "clickhouse"],
) as dag:
    t_extract = PythonOperator(task_id="extract_to_minio", python_callable=extract)
    t_load = PythonOperator(task_id="load_to_clickhouse", python_callable=load)

    t_extract >> t_load

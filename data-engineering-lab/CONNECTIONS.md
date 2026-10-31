# Подключения к лаборатории

Хост-машина: **192.168.0.13** (LAN). С самой машины вместо IP можно использовать `localhost`.

Единые учётки: **admin / admin**. Единственное исключение — MinIO: он технически
не принимает пароль короче 8 символов, поэтому там **admin / adminadmin**.

## 1. Адреса с хоста (и из локальной сети)

| Инструмент | Адрес | Логин / пароль | Примечание |
|---|---|---|---|
| Airflow UI | http://192.168.0.13:8080 | admin / admin | |
| Airflow REST API | http://192.168.0.13:8080/api/v1 | admin / admin | basic auth |
| JupyterLab | http://192.168.0.13:8888 | admin | это token/пароль |
| Spark UI | http://192.168.0.13:4040 | — | живёт, пока открыта Spark-сессия в Jupyter |
| MinIO Console | http://192.168.0.13:9001 | admin / adminadmin | веб-интерфейс |
| MinIO S3 API | http://192.168.0.13:9000 | admin / adminadmin | endpoint для boto3/Spark |
| PostgreSQL | 192.168.0.13:5432 | admin / admin | БД `airflow` (метаданные) и `lab` |
| ClickHouse `ch1-1` (shard 1 / replica 1) | http://192.168.0.13:8123 (native 9101) | admin / admin | |
| ClickHouse `ch1-2` (shard 1 / replica 2) | http://192.168.0.13:8124 (native 9102) | admin / admin | |
| ClickHouse `ch2-1` (shard 2 / replica 1) | http://192.168.0.13:8125 (native 9103) | admin / admin | |
| ClickHouse `ch2-2` (shard 2 / replica 2) | http://192.168.0.13:8126 (native 9104) | admin / admin | |
| ClickHouse Keeper | — | — | только внутри docker-сети, `keeper:9181` |

## 2. Адреса внутри docker-сети (для ноутбуков, DAG'ов, контейнеров)

| Сервис | Hostname:port |
|---|---|
| PostgreSQL | `postgres:5432` |
| MinIO | `minio:9000` |
| Airflow webserver | `airflow-webserver:8080` |
| ClickHouse `ch1-1` | `ch1-1:8123` / `ch1-1:9000` |
| ClickHouse `ch1-2` | `ch1-2:8123` / `ch1-2:9000` |
| ClickHouse `ch2-1` | `ch2-1:8123` / `ch2-1:9000` |
| ClickHouse `ch2-2` | `ch2-2:8123` / `ch2-2:9000` |
| ClickHouse Keeper | `keeper:9181` |

Имя docker-сети: `lab-net`.

## 3. ClickHouse: топология

Кластер: `lab_cluster`, 2 шарда × 2 реплики, `internal_replication = true`, координация через Keeper.

| Шард | Реплики |
|---|---|
| 1 | `ch1-1`, `ch1-2` |
| 2 | `ch2-1`, `ch2-2` |

Проверить топологию:

```sql
SELECT cluster, shard_num, replica_num, host_name, port
FROM system.clusters WHERE cluster = 'lab_cluster';
```

Создание replicated-таблицы на всём кластере:

```sql
CREATE DATABASE IF NOT EXISTS lab ON CLUSTER lab_cluster;

CREATE TABLE IF NOT EXISTS lab.events ON CLUSTER lab_cluster
(
    id     Int64,
    name   String,
    value  Float64,
    source String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/lab/events', '{replica}')
ORDER BY id;

CREATE TABLE IF NOT EXISTS lab.events_all ON CLUSTER lab_cluster
AS lab.events
ENGINE = Distributed('lab_cluster', 'lab', 'events', rand());
```

Запись напрямую в `lab.events` на одной ноде реплицируется на вторую реплику её шарда.
Чтение `lab.events_all` с любой ноды опрашивает все шарды.

Примеры подключения:

```bash
# HTTP
curl 'http://admin:admin@192.168.0.13:8123/?query=SELECT%201'

# native protocol
clickhouse-client --host 192.168.0.13 --port 9101 -u admin --password admin

# JDBC (DBeaver, Spark и т.п.)
jdbc:clickhouse://192.168.0.13:8123/lab
# драйвер: com.clickhouse.jdbc.ClickHouseDriver
```

```python
import clickhouse_connect

ch = clickhouse_connect.get_client(
    host="192.168.0.13", port=8123, username="admin", password="admin"
)
print(ch.query("SELECT version()").first_row)
```

## 4. MinIO / S3

Endpoint с хоста: `http://192.168.0.13:9000`
Endpoint из контейнеров: `http://minio:9000`
Access key: `admin`, Secret key: `adminadmin`
Бакеты: `warehouse`, `raw`, `processed`

```bash
# aws cli
aws --endpoint-url http://192.168.0.13:9000 s3 ls
```

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://192.168.0.13:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="adminadmin",
    region_name="us-east-1",
)
print([b["Name"] for b in s3.list_buckets()["Buckets"]])
```

S3A для Spark (из Jupyter используй `minio:9000`, с хоста — `192.168.0.13:9000`):

```
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=admin
spark.hadoop.fs.s3a.secret.key=adminadmin
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

Пример пути: `s3a://warehouse/demo/events`.

## 5. PostgreSQL

| БД | DSN с хоста |
|---|---|
| `airflow` | `postgresql://admin:admin@192.168.0.13:5432/airflow` |
| `lab` | `postgresql://admin:admin@192.168.0.13:5432/lab` |

```bash
psql "postgresql://admin:admin@192.168.0.13:5432/lab"
```

## 6. Airflow

- UI: http://192.168.0.13:8080 — admin / admin
- Пример DAG: `lab_etl_minio_to_clickhouse` (MinIO → ClickHouse), лежит в `airflow/dags/`
- Метаданные: Postgres, БД `airflow`
- Исполнитель: `LocalExecutor`

Пример DAG'а без UI (запуск вручную):

```bash
docker exec lab-airflow-scheduler airflow dags trigger lab_etl_minio_to_clickhouse
```

## 7. Spark из Jupyter

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("de-lab")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
```

Jar'ы `hadoop-aws`, `aws-java-sdk-bundle` и `clickhouse-jdbc` уже лежат в `$SPARK_HOME/jars`,
отдельно указывать `--packages` не нужно. Версия JDBC-драйвера — `0.6.0-all`
(в 0.8.x другой ANTLR, который конфликтует с ANTLR 4.9.3 внутри Spark 3.5).

## 8. Быстрая проверка всего

```bash
curl -s -o /dev/null -w 'airflow %{http_code}\n'  http://192.168.0.13:8080/health
curl -s -o /dev/null -w 'jupyter %{http_code}\n'  http://192.168.0.13:8888
curl -s -o /dev/null -w 'minio   %{http_code}\n'  http://192.168.0.13:9000/minio/health/live
curl -s -o /dev/null -w 'ch1-1   %{http_code}\n'  http://192.168.0.13:8123/ping
curl -s -o /dev/null -w 'ch1-2   %{http_code}\n'  http://192.168.0.13:8124/ping
curl -s -o /dev/null -w 'ch2-1   %{http_code}\n'  http://192.168.0.13:8125/ping
curl -s -o /dev/null -w 'ch2-2   %{http_code}\n'  http://192.168.0.13:8126/ping
```

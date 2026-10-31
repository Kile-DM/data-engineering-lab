# Лаборатория дата-инжиниринга

Локальный стенд в Docker: Airflow 2.10.5, Spark 3.5.3 (локальный режим в Jupyter),
PostgreSQL, MinIO (S3) и ClickHouse 2 шарда × 2 реплики.

Все адреса и учётки — в [CONNECTIONS.md](./CONNECTIONS.md).

## Состав

| Сервис | Версия | Порт(ы) |
|---|---|---|
| Airflow (webserver + scheduler, LocalExecutor) | 2.10.5 | 8080 |
| PostgreSQL | 16 | 5432 |
| MinIO (S3 API / Console) | latest | 9000 / 9001 |
| JupyterLab + PySpark | Spark 3.5.3 | 8888, 4040 |
| ClickHouse (2 шарда × 2 реплики) | 24.8 | 8123–8126, native 9101–9104 |
| ClickHouse Keeper | 24.8 | только внутри сети |

## Требования

- Docker Engine + плагин `docker compose` (v2)
- ~10 ГБ свободной оперативной памяти
- Свободные порты: 5432, 8080, 8888, 9000, 9001, 4040, 8123–8126, 9101–9104

## Запуск

```bash
cd ~/data-engineering-lab
docker compose up -d --build
```

Первый запуск занимает несколько минут: скачиваются образы (Airflow ~1.5 ГБ,
Jupyter + Spark ~2 ГБ) и ставятся python-пакеты.

Проверить статус:

```bash
docker compose ps
```

Когда все контейнеры `healthy` / `running`:

- Airflow — http://192.168.0.13:8080 (admin / admin)
- JupyterLab — http://192.168.0.13:8888 (пароль/token `admin`)
- MinIO Console — http://192.168.0.13:9001 (admin / adminadmin)

## Первый сценарий

1. Открой JupyterLab, ноутбук `work/lab_pyspark_minio_clickhouse.ipynb`.
2. Выполни ячейки сверху вниз: Spark пишет Parquet в MinIO, затем DataFrame
   уходит в ClickHouse по JDBC, в конце проверяется репликация и distributed-чтение.
3. Открой Airflow UI, включи (или дождись) DAG `lab_etl_minio_to_clickhouse` —
   он делает похожий ETL: генерирует данные → MinIO → ClickHouse.

## Остановка и сброс

```bash
# остановить, данные сохраняются
docker compose stop

# остановить и удалить контейнеры (данные в томах остаются)
docker compose down

# полностью снести лабораторию вместе с данными
docker compose down -v
```

## Структура

```
data-engineering-lab/
├── docker-compose.yml
├── .env                    # пароли и секреты
├── CONNECTIONS.md          # все IP/порты/логины
├── README.md
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/lab_etl_minio_to_clickhouse.py
├── jupyter/
│   └── Dockerfile          # PySpark + jar'ы S3A/ClickHouse JDBC
├── clickhouse/
│   ├── keeper/keeper_config.xml
│   ├── ch1-1/ ch1-2/ ch2-1/ ch2-2/   # config.d + users.d
├── postgres/init.sql
└── notebooks/lab_pyspark_minio_clickhouse.ipynb
```

## Если что-то не поднимается

```bash
# логи конкретного сервиса
docker compose logs -f airflow-scheduler
docker compose logs -f ch1-1

# перезапуск одного сервиса
docker compose restart jupyter
```

Типичные причины:

- **порт занят** — проверь `ss -ltn | grep -E '8080|8888|9000|8123'` и поправь
  маппинг в `docker-compose.yml`;
- **не хватает памяти** — уменьши `mem_limit` у ClickHouse/Jupyter или закрой
  лишние приложения;
- **нет доступа к docker** — пользователь должен быть в группе `docker`
  (см. ниже) либо команды запускаются через `sudo`.

### Доступ к Docker без sudo

```bash
sudo usermod -aG docker "$USER"
newgrp docker          # или перелогиниться
```

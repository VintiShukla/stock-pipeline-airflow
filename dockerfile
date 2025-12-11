FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && apt-get install -y build-essential libpq-dev

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# dags are mounted from host via docker-compose volumes

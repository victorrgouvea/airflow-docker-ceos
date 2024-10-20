FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y openjdk-17-jre

USER airflow
# Install dependencies from requirements.txt
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

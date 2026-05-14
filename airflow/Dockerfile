FROM apache/airflow:2.10.4-python3.11

USER root

# Airflow a besoin de Java pour lancer les jobs Spark.
# IMPORTANT: Doit utiliser Java 17 pour être compatible avec les conteneurs Spark (spark:3.5.8-scala2.12-java17-python3-ubuntu)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY --chown=airflow:root airflow/requirements.txt /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root spark_trainer/train_model.py /opt/airflow/jobs/train_model.py

# On expose le script de training à Airflow via PYTHONPATH.
ENV PYTHONPATH=/opt/airflow/jobs

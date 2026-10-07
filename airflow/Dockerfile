FROM apache/airflow:3.3.2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir pyspark==4.0.4   # same version as the cluster

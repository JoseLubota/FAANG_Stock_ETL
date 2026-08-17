# syntax=docker/dockerfile:1
FROM apache/airflow:3.3.0
USER root

# Install Java (required by PySpark) and procps (required by spark-env scripts)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow
WORKDIR /opt/airflow

# Install pyspark, findspark, logger, and other required packages
RUN pip install --no-cache-dir pyspark==3.5.2 findspark logger yfinance pandas apache-airflow-providers-postgres

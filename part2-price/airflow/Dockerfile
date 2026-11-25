# Spark 클러스터(spark://spark-master:7077)에 작업을 제출할 수 있는 Airflow 이미지
#  - Python 3.8 : apache/spark:3.5.3(Ubuntu 20.04, Python 3.8) 의 executor 와 driver Python 버전을 맞춤
#  - Java       : spark-submit(driver) 실행용 JRE
#  - pyspark    : 클러스터와 같은 3.5.3 (spark-submit 바이너리 포함)
FROM apache/airflow:2.10.5-python3.8

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.8
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-apache-spark==4.11.0" \
      "confluent-kafka==2.8.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
 && pip install --no-cache-dir "pyspark==3.5.3"

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

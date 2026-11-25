"""Q9 — Airflow 스케줄 자동화 + Athena 쿼리 ID 교차검증

DAG price_pipeline_kjh (10분 주기, 실행 겹침 없음: max_active_runs=1)
  Task 1 spark_price_batch   : SparkSubmitOperator 로 price_batch.py(Q7·Q8) 를 Spark 클러스터에 제출
                               → S3 bronze/silver/gold(grocery_price_change) 갱신
  Task 2 register_gold_table : Athena DB de9_kjh 에 Q8 Gold 경로를 가리키는 외부 테이블 등록
  Task 3 query_top1          : 도시별 변동률 1위 품목 SELECT 를 Athena 에서 실행
                               → AthenaOperator 가 쿼리 실행 ID 를 반환 = XCom(return_value)

AWS 접근은 EC2 IAM Role(de9-ec2-kjh) — aws_conn_id=None 으로 boto3 기본 자격 증명 체인(IMDS) 사용.
Spark 연결은 compose 의 AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077?deploy-mode=client
"""
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BUCKET = "de9-kimjunghyun"
REGION = "ap-northeast-2"
ATHENA_DB = "de9_kjh"
ATHENA_TABLE = "grocery_price_change"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"
WORKGROUP = "primary"

default_args = {
    "owner": "kjh",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}

with DAG(
    dag_id="price_pipeline_kjh",
    description="Q9: Kafka product-prices → Spark batch(bronze/silver/gold) → Athena 1위 품목 조회",
    schedule="*/10 * * * *",                      # 10분 주기
    start_date=pendulum.datetime(2026, 11, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,                            # 실행이 겹치지 않게
    dagrun_timeout=pendulum.duration(minutes=9),
    default_args=default_args,
    tags=["de9", "kjh", "q9"],
) as dag:

    spark_price_batch = SparkSubmitOperator(
        task_id="spark_price_batch",
        conn_id="spark_default",
        application="/opt/work/part2-price/price_batch.py",
        name="price_batch_kjh",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.driver.host": "airflow",       # executor(spark-worker) 가 driver(airflow 컨테이너)로 접속
            "spark.jars.ivy": "/home/airflow/.ivy2",
        },
        driver_memory="512m",
        executor_memory="1g",
        total_executor_cores=2,
        verbose=False,
    )

    register_gold_table = AthenaOperator(
        task_id="register_gold_table",
        aws_conn_id=None,
        region_name=REGION,
        workgroup=WORKGROUP,
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT,
        sleep_time=3,
        query=f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE} (
                market       string,
                item_name    string,
                price_nov    double,
                price_dec    double,
                change_rate  double,
                change_rank  int
            )
            STORED AS PARQUET
            LOCATION 's3://{BUCKET}/gold/{ATHENA_TABLE}/'
        """,
    )

    query_top1 = AthenaOperator(                  # 반환값(쿼리 실행 ID) → XCom return_value
        task_id="query_top1",
        aws_conn_id=None,
        region_name=REGION,
        workgroup=WORKGROUP,
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT,
        sleep_time=3,
        query=f"""
            SELECT market, item_name, price_nov, price_dec, change_rate, change_rank
            FROM {ATHENA_DB}.{ATHENA_TABLE}
            WHERE change_rank = 1
            ORDER BY market
        """,
    )

    spark_price_batch >> register_gold_table >> query_top1

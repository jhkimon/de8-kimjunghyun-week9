"""Q7 — Spark 배치: Kafka product-prices → S3 Bronze/Silver + 품목별·월별 가격 집계

Kafka 토픽에 있는 데이터를 배치로 한 번(earliest ~ latest) 읽어 처리하고 종료합니다.
(Q9 에서 Airflow 가 주기적으로 실행 — 매 실행이 토픽 전체를 다시 읽으므로 overwrite 로 멱등 적재)

  - Bronze : 원본 메시지 그대로(key/value 문자열 + Kafka 메타데이터) → s3a://{버킷}/bronze/product-prices/
  - Silver : JSON 파싱(명시적 스키마), price 실수형(double), price 가 없거나 0 이하인 레코드 제외
             → s3a://{버킷}/silver/product-prices/ (parquet)
  - 콘솔   : 품목별·월별(item_name, month) 평균가·최고가·최저가

S3 접근은 EC2 IAM Role(de9-ec2-kjh) — 액세스 키 없음.
실행 (EC2 호스트):
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 /opt/work/part2-price/price_batch.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BUCKET = "de9-kimjunghyun"
TOPIC = "product-prices"
KAFKA_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"

BRONZE_PATH = f"s3a://{BUCKET}/bronze/{TOPIC}/"
SILVER_PATH = f"s3a://{BUCKET}/silver/{TOPIC}/"

# 명시적 스키마 — price 는 문자열/숫자 어느 쪽으로 와도 받도록 string 으로 읽은 뒤 double 로 변환
PRICE_SCHEMA = StructType([
    StructField("item_name", StringType(), True),
    StructField("market", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("price", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("unit", StringType(), True),
])


def build_spark():
    spark = (SparkSession.builder
             .appName("price_batch_kjh")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.InstanceProfileCredentialsProvider")   # EC2 IAM Role
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_batch(spark):
    """배치 읽기: 토픽의 처음부터 현재 끝까지 한 번."""
    return (spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load())


def to_bronze(raw_df):
    """가공 없는 원본 메시지 + Kafka 메타데이터."""
    return raw_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafkaTimestamp"),
    )


def to_silver(bronze_df):
    """JSON 파싱 → price 실수형 → 결측·0 이하 제외."""
    parsed = (bronze_df
              .select(F.from_json("value", PRICE_SCHEMA).alias("r"), "partition", "offset")
              .select("r.*", F.col("partition").alias("kafkaPartition"), F.col("offset").alias("kafkaOffset"))
              .withColumn("price", F.col("price").cast(DoubleType())))
    return parsed.filter(F.col("price").isNotNull() & (F.col("price") > 0))


def item_month_stats(silver_df):
    return (silver_df.groupBy("item_name", "month")
            .agg(F.round(F.avg("price"), 2).alias("avg_price"),
                 F.max("price").alias("max_price"),
                 F.min("price").alias("min_price"))
            .orderBy("item_name", "month"))


def main():
    spark = build_spark()

    raw = read_kafka_batch(spark)
    bronze = to_bronze(raw).cache()
    bronze_cnt = bronze.count()
    bronze.write.mode("overwrite").parquet(BRONZE_PATH)
    print(f"Bronze 적재: {bronze_cnt}건 -> {BRONZE_PATH}", flush=True)

    silver = to_silver(bronze).cache()
    silver_cnt = silver.count()
    silver.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"Silver 적재: {silver_cnt}건 -> {SILVER_PATH} (제외 {bronze_cnt - silver_cnt}건: price 결측·0 이하)",
          flush=True)

    print("=== Q7 품목별 월별 평균/최고/최저가 ===", flush=True)
    item_month_stats(silver).show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

"""Q4 — Spark Structured Streaming: Kafka → S3 Bronze/Silver + 설치구역별 누적 집계 콘솔 출력

Kafka ev-charging-sessions 를 readStream 으로 읽어
  - Bronze : 가공 없는 원본 이벤트(Kafka value 문자열 + 메타데이터) → s3a://{버킷}/bronze/ev-charging-sessions/dt=YYYY-MM-DD/
  - Silver : 명시적 스키마로 파싱 + locationGroup / totalSec / chargingSec / idleSec
             → s3a://{버킷}/silver/ev-charging-sessions/dt=YYYY-MM-DD/
  두 적재 모두 하나의 foreachBatch 에서 parquet(append) 로 수행합니다.
  - 콘솔  : 설치구역(locationGroup)별 누적 세션 수 (complete 모드, truncate=false) — 트리거마다 출력

S3 접근은 EC2 IAM Role(de9-ec2-kjh) — 코드·설정 어디에도 액세스 키 없음.
실행 (EC2 호스트, spark-master 컨테이너에서 driver 실행):
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 /opt/work/part1-ev/charger_stream.py
"""
import argparse
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BUCKET = "de9-kimjunghyun"
TOPIC = "ev-charging-sessions"
KAFKA_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"

BRONZE_PATH = f"s3a://{BUCKET}/bronze/{TOPIC}/"
SILVER_PATH = f"s3a://{BUCKET}/silver/{TOPIC}/"
CHECKPOINT_ROOT = f"s3a://{BUCKET}/_checkpoints/{TOPIC}"

# 명시적 스키마 (Producer 메시지 JSON 스키마와 동일)
SESSION_SCHEMA = StructType([
    StructField("stationName", StringType(), False),
    StructField("portNumber", IntegerType(), True),
    StructField("portType", StringType(), True),
    StructField("plugType", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("totalDuration", StringType(), True),
    StructField("chargingDuration", StringType(), True),
    StructField("energyKwh", DoubleType(), True),
    StructField("endedBy", StringType(), True),
])


def hms_to_sec(col):
    """'h:mm:ss' → 초 (int)."""
    parts = F.split(col, ":")
    return (parts.getItem(0).cast("int") * 3600
            + parts.getItem(1).cast("int") * 60
            + parts.getItem(2).cast("int"))


def parse_sessions(raw_df):
    """Kafka value(JSON) → 명시적 스키마로 파싱한 세션 컬럼."""
    return (raw_df
            .select(F.from_json(F.col("value").cast("string"), SESSION_SCHEMA).alias("s"),
                    F.col("partition").alias("kafkaPartition"),
                    F.col("offset").alias("kafkaOffset"))
            .select("s.*", "kafkaPartition", "kafkaOffset"))


def to_silver(parsed_df):
    return (parsed_df
            .withColumn("locationGroup", F.regexp_replace("stationName", r" #\d+$", ""))
            .withColumn("startTs", F.to_timestamp("startTime", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("endTs", F.to_timestamp("endTime", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("totalSec", hms_to_sec(F.col("totalDuration")))
            .withColumn("chargingSec", hms_to_sec(F.col("chargingDuration")))
            .withColumn("idleSec", F.col("totalSec") - F.col("chargingSec"))
            .withColumn("dt", F.substring("startTime", 1, 10)))


def write_bronze_silver(batch_df, batch_id):
    """foreachBatch: 같은 마이크로배치를 Bronze(원본) / Silver(정제) 로 이중 적재."""
    batch_df.persist()
    try:
        n = batch_df.count()
        if n == 0:
            return
        # Bronze — 원본 그대로 (value 문자열 + Kafka 메타데이터), 파티션 컬럼 dt 만 추가
        bronze = (batch_df
                  .select(F.col("key").cast("string").alias("key"),
                          F.col("value").cast("string").alias("value"),
                          "topic", "partition", "offset",
                          F.col("timestamp").alias("kafkaTimestamp"))
                  .withColumn("dt", F.get_json_object("value", "$.startTime").substr(1, 10)))
        bronze.write.mode("append").partitionBy("dt").parquet(BRONZE_PATH)

        # Silver — 파싱 + 파생 컬럼
        silver = to_silver(parse_sessions(batch_df))
        silver.write.mode("append").partitionBy("dt").parquet(SILVER_PATH)
        print(f"[foreachBatch {batch_id}] bronze/silver 적재 {n}건", flush=True)
    finally:
        batch_df.unpersist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="10 seconds", help="마이크로배치 트리거 간격")
    ap.add_argument("--max-offsets", type=int, default=60, help="트리거당 최대 Kafka 오프셋 수")
    ap.add_argument("--idle-stop", type=int, default=3,
                    help="데이터를 처리한 뒤 입력 0건 트리거가 이만큼 이어지면 종료 (0=계속 실행)")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("charger_stream_kjh")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.InstanceProfileCredentialsProvider")   # EC2 IAM Role
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def kafka_stream():
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", args.max_offsets)
                .option("failOnDataLoss", "false")
                .load())

    # 1) Bronze/Silver 이중 적재
    q_store = (kafka_stream().writeStream
               .queryName("bronze_silver")
               .foreachBatch(write_bronze_silver)
               .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_silver")
               .trigger(processingTime=args.trigger)
               .start())

    # 2) 설치구역별 누적 세션 수 → 콘솔 (complete 모드 = 누적, truncate=false = 이름 안 잘림)
    agg = (to_silver(parse_sessions(kafka_stream()))
           .groupBy("locationGroup").count()
           .orderBy(F.desc("count"), "locationGroup"))
    q_console = (agg.writeStream
                 .queryName("location_group_count")
                 .outputMode("complete")
                 .format("console")
                 .option("truncate", "false")
                 .option("numRows", 50)
                 .option("checkpointLocation", f"{CHECKPOINT_ROOT}/console")
                 .trigger(processingTime=args.trigger)
                 .start())

    queries = [q_store, q_console]
    if args.idle_stop <= 0:
        spark.streams.awaitAnyTermination()
        return

    # 토픽 끝까지 따라잡은(endOffset == latestOffset) 상태가 idle_stop 번 확인되면 정상 종료 (캡처용)
    def caught_up(q):
        p = q.lastProgress
        if not p or not p.get("sources") or q.status["isTriggerActive"]:
            return False
        src = p["sources"][0]
        return src.get("endOffset") is not None and src.get("endOffset") == src.get("latestOffset")

    streak = 0
    while all(q.isActive for q in queries):
        time.sleep(3)
        streak = streak + 1 if all(caught_up(q) for q in queries) else 0
        if streak >= args.idle_stop:
            break
    for q in queries:
        if q.exception():
            raise q.exception()
    processed = {q.name: sum(p["numInputRows"] for p in q.recentProgress) for q in queries}
    for q in queries:
        q.stop()
    print(f"[종료] 토픽 끝까지 처리 완료 — 처리 건수 {processed}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()

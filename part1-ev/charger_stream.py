"""Q4·Q5 — Spark Structured Streaming: Kafka → S3 Bronze/Silver/Gold + 콘솔 집계 + Slack 이상 알림

Kafka ev-charging-sessions 를 readStream 으로 읽어
  - Bronze : 가공 없는 원본 이벤트(Kafka value 문자열 + 메타데이터) → s3a://{버킷}/bronze/ev-charging-sessions/dt=YYYY-MM-DD/
  - Silver : 명시적 스키마로 파싱 + locationGroup / totalSec / chargingSec / idleSec
             → s3a://{버킷}/silver/ev-charging-sessions/dt=YYYY-MM-DD/
  두 적재 모두 하나의 foreachBatch 에서 parquet(append) 로 수행합니다.
  - 콘솔  : 설치구역(locationGroup)별 누적 세션 수 (complete 모드, truncate=false) — 트리거마다 출력
  [Q5] 같은 foreachBatch 에서 이어서
  - 감지  : Silver idleSec > 5400(90분) = 포트 점유 이상(완충 후 미방출) 세션
  - Slack : 감지 세션마다 Incoming Webhook 으로 알림 (stationName, locationGroup, startTime, idleSec)
            웹훅 URL 은 환경변수 SLACK_WEBHOOK_URL 또는 part1-ev/.env (git 제외) 에서 읽음
  - Gold  : Silver 전체 기준 설치구역별 이상 건수 집계 → s3a://{버킷}/gold/ev_charging_agg/ (overwrite)

S3 접근은 EC2 IAM Role(de9-ec2-kjh) — 코드·설정 어디에도 액세스 키 없음.
실행 (EC2 호스트, spark-master 컨테이너에서 driver 실행):
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 /opt/work/part1-ev/charger_stream.py
"""
import argparse
import json
import os
import time
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BUCKET = "de9-kimjunghyun"
TOPIC = "ev-charging-sessions"
KAFKA_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"

BRONZE_PATH = f"s3a://{BUCKET}/bronze/{TOPIC}/"
SILVER_PATH = f"s3a://{BUCKET}/silver/{TOPIC}/"
GOLD_PATH = f"s3a://{BUCKET}/gold/ev_charging_agg/"
CHECKPOINT_ROOT = f"s3a://{BUCKET}/_checkpoints/{TOPIC}"
IDLE_THRESHOLD_SEC = 5400          # 90분

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


def slack_webhook_url():
    url = os.getenv("SLACK_WEBHOOK_URL")
    if url:
        return url
    env_file = Path(__file__).resolve().parent / ".env"          # git 제외 파일
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("SLACK_WEBHOOK_URL="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def send_slack(text):
    url = slack_webhook_url()
    if not url:
        print("  (SLACK_WEBHOOK_URL 없음 — 전송 생략)", flush=True)
        return False
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode("utf-8"),
                                 headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return True
        except Exception as e:                       # 429 등 → 잠시 후 재시도
            print(f"  Slack 전송 재시도 {attempt + 1}: {e}", flush=True)
            time.sleep(2)
    return False


def alert_idle_sessions(silver_df, batch_id):
    """[Q5] Silver idleSec > 90분 세션 감지 → 세션마다 Slack 알림."""
    rows = (silver_df.filter(F.col("idleSec") > IDLE_THRESHOLD_SEC)
            .select("stationName", "locationGroup", "startTime", "idleSec")
            .orderBy("startTime", "stationName")
            .collect())
    sent = 0
    for r in rows:
        text = (f"[포트 점유 이상] {r.stationName} (설치구역: {r.locationGroup} / "
                f"시작 {r.startTime} / idleSec={r.idleSec})")
        if send_slack(text):
            sent += 1
            time.sleep(1.1)                          # Slack 웹훅 rate limit(초당 1건)
    print(f"[foreachBatch {batch_id}] 포트 점유 이상 감지 {len(rows)}건 → Slack 전송 {sent}건", flush=True)
    return len(rows)


def write_gold(spark):
    """[Q5] Silver 전체(재처리 중복 제거) 기준 설치구역별 포트 점유 이상 건수 → Gold overwrite (parquet 1개)."""
    silver_all = (spark.read.parquet(SILVER_PATH)
                  .dropDuplicates(["kafkaPartition", "kafkaOffset"]))
    gold = (silver_all.filter(F.col("idleSec") > IDLE_THRESHOLD_SEC)
            .groupBy("locationGroup")
            .agg(F.count("*").cast("int").alias("anomaly_count"),
                 F.max("idleSec").alias("max_idle_sec"))
            .orderBy(F.desc("anomaly_count"), "locationGroup"))
    gold.coalesce(1).write.mode("overwrite").parquet(GOLD_PATH)
    return gold.agg(F.sum("anomaly_count")).first()[0]


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

        # [Q5] 포트 점유 이상 감지 → Slack, Gold 갱신
        alert_idle_sessions(silver, batch_id)
        total_anomaly = write_gold(batch_df.sparkSession)
        print(f"[foreachBatch {batch_id}] gold 갱신 — 누적 이상 {total_anomaly}건", flush=True)
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

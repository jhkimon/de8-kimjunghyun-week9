"""Q6 — 식료품 가격 Producer · 권역별 커스텀 파티셔너 (kafka-python)

data/india_grocery_prices.csv 200행을 product-prices 토픽(파티션 3, 복제 2)으로 전송합니다.
메시지 키 = market(도시명). 파티션은 send() 에서 지정하지 않고, Producer 에 등록한
커스텀 파티셔너(RegionPartitioner)가 도시가 속한 권역으로 정합니다.

  파티션 0  북부       Delhi · Jaipur · Lucknow · Kanpur
  파티션 1  서부·중부  Mumbai · Ahmedabad · Bhopal
  파티션 2  남부·동부  Hyderabad · Chennai · Patna
  표에 없는 도시 → 기본 파티셔너(murmur2 해시) 규칙

kafka-python 3.x 는 kafka.partitioner.Partitioner 를 상속해 partition(...) 을 구현하고
KafkaProducer(partitioner=인스턴스) 로 지정합니다.

실행 (EC2 호스트):  ~/venv/bin/python part2-price/price_producer.py
"""
import argparse
import csv
import json
import os
import sys
from collections import Counter
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.partitioner import DefaultPartitioner
from kafka.serializer import Serializer

TOPIC = "product-prices"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092,localhost:39092,localhost:49092").split(",")
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "india_grocery_prices.csv"

REGION_PARTITION = {
    # 0: 북부
    "Delhi": 0, "Jaipur": 0, "Lucknow": 0, "Kanpur": 0,
    # 1: 서부·중부
    "Mumbai": 1, "Ahmedabad": 1, "Bhopal": 1,
    # 2: 남부·동부
    "Hyderabad": 2, "Chennai": 2, "Patna": 2,
}
REGION_NAME = {0: "북부", 1: "서부·중부", 2: "남부·동부"}


class RegionPartitioner(DefaultPartitioner):
    """도시(메시지 키) → 권역 파티션. 표에 없는 도시는 DefaultPartitioner(murmur2) 로 위임."""

    def partition(self, topic, key, serialized_key, value, serialized_value, cluster):
        all_partitions = sorted(cluster.partitions_for_topic(topic) or [])
        city = serialized_key.decode("utf-8") if serialized_key is not None else None
        p = REGION_PARTITION.get(city)
        if p is not None and p in all_partitions:
            return p
        return super().partition(topic, key, serialized_key, value, serialized_value, cluster)


class StrSerializer(Serializer):
    def serialize(self, topic, value):
        return value.encode("utf-8")


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value, ensure_ascii=False).encode("utf-8")


def load_rows(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        return [
            {
                "item_name": r["item_name"],
                "market": r["market"],
                "year": int(r["year"]),
                "month": int(r["month"]),
                "price": float(r["price"]),
                "currency": r["currency"],
                "unit": r["unit"],
            }
            for r in csv.DictReader(f)
        ]


def topic_message_count(topic: str) -> int:
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False)
    try:
        parts = c.partitions_for_topic(topic)
        if not parts:
            raise SystemExit(f"[중단] 토픽 {topic} 이 없습니다. 먼저 partitions 3 / RF 2 로 생성하세요.")
        tps = [TopicPartition(topic, p) for p in parts]
        end, begin = c.end_offsets(tps), c.beginning_offsets(tps)
        return sum(end[tp] - begin[tp] for tp in tps)
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="토픽에 메시지가 있어도 전송 (중복 주의)")
    args = ap.parse_args()

    rows = load_rows(CSV_PATH)
    existing = topic_message_count(TOPIC)
    if existing and not args.force:
        print(f"[중단] {TOPIC} 에 이미 {existing}건이 있습니다. 중복 적재를 막기 위해 전송하지 않습니다.")
        return 1

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        client_id="price-producer-kjh",
        partitioner=RegionPartitioner(),          # 커스텀 파티셔너 지정
        key_serializer=StrSerializer(),
        value_serializer=JsonSerializer(),
        acks="all",                               # 유실 방지
        enable_idempotence=True,                  # 재시도 중복 방지
        retries=10,
        linger_ms=5,
    )
    per_partition: Counter = Counter()
    per_city_partition = {}
    failed = 0
    try:
        futures = [(r["market"], producer.send(TOPIC, key=r["market"], value=r)) for r in rows]  # partition 미지정
        producer.flush()
        for city, fut in futures:
            try:
                md = fut.get(timeout=30)
            except Exception as e:  # 전송 실패
                failed += 1
                print(f"  !! 전송 실패 {city}: {e}", file=sys.stderr)
                continue
            per_partition[md.partition] += 1
            per_city_partition.setdefault(md.partition, set()).add(city)
    finally:
        producer.flush()
        producer.close()

    print(f"전송 완료: {sum(per_partition.values())}건 -> topic={TOPIC} (실패 {failed}건)")
    for p in sorted(per_partition):
        cities = ", ".join(sorted(per_city_partition[p]))
        print(f"  partition {p} ({REGION_NAME.get(p, '기본 규칙')}): {per_partition[p]}건  [{cities}]")
    return 0 if failed == 0 and sum(per_partition.values()) == len(rows) else 2


if __name__ == "__main__":
    sys.exit(main())

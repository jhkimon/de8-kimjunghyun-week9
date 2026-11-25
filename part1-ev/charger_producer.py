"""Q2 — 충전 세션 Producer (실측 로그 replay)

data/charger_sessions.csv 를 startTime 순으로 정렬해 한 행씩 0.05~0.2초 간격으로
EC2 위 Kafka 토픽 ev-charging-sessions 로 전송합니다.

- 유실 방지 : acks=all + 무한에 가까운 재시도(delivery.timeout 안에서) — 리더 broker 1대가 죽어도
              나머지 replica 가 리더를 이어받으면 재전송되어 커밋됨 (토픽 RF=2)
- 중복 방지 : enable.idempotence=true — 재시도로 같은 배치가 다시 가도 broker 가 PID/시퀀스로 걸러 냄
- 파티션 키 : key=stationName, murmur2 파티셔너 → 같은 충전기는 항상 같은 파티션
- 재실행 방지: 토픽에 이미 메시지가 있으면 전송하지 않음 (중복 적재 → Q3~Q5 정답 불일치 방지)

실행 (EC2 호스트):  ~/venv/bin/python part1-ev/charger_producer.py
"""
import argparse
import csv
import json
import os
import random
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

TOPIC = "ev-charging-sessions"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092,localhost:39092,localhost:49092")
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "charger_sessions.csv"

PRODUCER_CONF = {
    "bootstrap.servers": BOOTSTRAP,
    "client.id": "charger-producer-kjh",
    # 유실 방지
    "acks": "all",
    "retries": 2147483647,
    "delivery.timeout.ms": 120000,
    "request.timeout.ms": 30000,
    "retry.backoff.ms": 200,
    # 중복 방지 (멱등 Producer: in-flight ≤ 5 에서도 순서·단일 저장 보장)
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    # 같은 key → 같은 파티션 (Java 클라이언트와 동일한 해시)
    "partitioner": "murmur2_random",
    "linger.ms": 5,
}


def load_sessions(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # startTime 순 정렬 (형식이 'YYYY-MM-DD HH:MM:SS' 이므로 문자열 정렬 = 시간 정렬, 동률은 파일 순서 유지)
    rows.sort(key=lambda r: r["startTime"])
    return [
        {
            "stationName": r["stationName"],
            "portNumber": int(r["portNumber"]),
            "portType": r["portType"],
            "plugType": r["plugType"],
            "startTime": r["startTime"],
            "endTime": r["endTime"],
            "totalDuration": r["totalDuration"],
            "chargingDuration": r["chargingDuration"],
            "energyKwh": float(r["energyKwh"]),
            "endedBy": r["endedBy"],
        }
        for r in rows
    ]


def topic_message_count(topic: str) -> int:
    """토픽의 전체 메시지 수(파티션별 high - low watermark 합)."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "charger-producer-precheck"})
    try:
        md = c.list_topics(topic, timeout=10)
        if md.topics[topic].error is not None:
            raise KafkaException(md.topics[topic].error)
        total = 0
        for p in md.topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            total += hi - lo
        return total
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="토픽에 메시지가 있어도 전송 (중복 주의)")
    args = ap.parse_args()

    sessions = load_sessions(CSV_PATH)
    existing = topic_message_count(TOPIC)
    if existing and not args.force:
        print(f"[중단] {TOPIC} 에 이미 {existing}건이 있습니다. 중복 적재를 막기 위해 전송하지 않습니다.")
        print("       다시 보내려면 토픽을 삭제 후 재생성(partitions 3, RF 2)하고 실행하세요.")
        return 1

    print(f"bootstrap={BOOTSTRAP} topic={TOPIC} rows={len(sessions)} (startTime 정렬)")
    stats = {"ok": 0, "fail": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["fail"] += 1
            print(f"  !! 전송 실패 key={msg.key().decode()} err={err}", file=sys.stderr)
            return
        stats["ok"] += 1
        v = json.loads(msg.value())
        print(f"  [{stats['ok']:>3}] p{msg.partition()} off={msg.offset():<3} "
              f"{v['startTime']}  {v['stationName']} #{v['portNumber']}  {v['endedBy']}")

    producer = Producer(PRODUCER_CONF)
    try:
        for s in sessions:
            while True:
                try:
                    producer.produce(
                        TOPIC,
                        key=s["stationName"].encode("utf-8"),
                        value=json.dumps(s, ensure_ascii=False).encode("utf-8"),
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:  # 로컬 큐가 가득 차면 비워질 때까지 대기 후 재시도
                    producer.poll(0.5)
            producer.poll(0)  # 전송 결과 콜백 처리
            time.sleep(random.uniform(0.05, 0.2))
    finally:
        remaining = producer.flush(60)
        producer.close()
        print(f"flush 후 미전송 {remaining}건, 실패 {stats['fail']}건")

    print(f"전송 완료: {stats['ok']}건 -> topic={TOPIC}")
    return 0 if stats["ok"] == len(sessions) and stats["fail"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

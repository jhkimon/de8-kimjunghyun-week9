"""Q10 — 컨슈머 그룹 lag 모니터

그룹 이름을 인자로 받아 파티션별 topic, partition, current-offset, log-end-offset, lag 와
lag 합계를 표로 출력합니다. 그룹에 가입(subscribe)하지 않는 조회 전용이라 그룹 상태·오프셋을 바꾸지 않습니다.

  current-offset : 그룹이 커밋한 오프셋 (KafkaAdminClient.list_group_offsets)
  log-end-offset : 파티션의 끝 오프셋     (KafkaConsumer.end_offsets, group_id 없음)
  lag            : log-end-offset - current-offset (커밋 기록이 없으면 log-end-offset - log-start-offset)

실행 (EC2 호스트):  ~/venv/bin/python part2-price/kafka_lag_monitor.py ev-consumer-kjh
"""
import argparse
import os
import sys

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092,localhost:39092,localhost:49092").split(",")


def main() -> int:
    ap = argparse.ArgumentParser(description="Kafka 컨슈머 그룹 파티션별 lag 조회")
    ap.add_argument("group", help="컨슈머 그룹 이름 (예: ev-consumer-kjh)")
    ap.add_argument("--topic", help="이 토픽만 조회 (생략 시 그룹이 커밋한 모든 토픽)")
    args = ap.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="lag-monitor-kjh")
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, enable_auto_commit=False)
    try:
        # kafka-python 3.x: list_group_offsets({group: None}) → {group: {TopicPartition: OffsetAndMetadata}}
        committed = admin.list_group_offsets({args.group: None}).get(args.group, {})
        topics = {args.topic} if args.topic else {tp.topic for tp in committed}
        if not topics:
            print(f"group = {args.group} : 커밋된 오프셋이 없습니다. --topic 으로 토픽을 지정하세요.")
            return 1

        tps = sorted(
            (TopicPartition(t, p) for t in topics for p in (consumer.partitions_for_topic(t) or [])),
            key=lambda tp: (tp.topic, tp.partition),
        )
        end = consumer.end_offsets(tps)
        begin = consumer.beginning_offsets(tps)

        rows, total = [], 0
        for tp in tps:
            meta = committed.get(tp)
            cur = meta.offset if meta is not None and meta.offset >= 0 else None
            lag = end[tp] - (cur if cur is not None else begin[tp])
            total += lag
            rows.append((tp.topic, tp.partition, "-" if cur is None else cur, end[tp], lag))
    finally:
        consumer.close()
        admin.close()

    w = max([len("topic"), len("TOTAL LAG")] + [len(r[0]) for r in rows]) + 2
    print(f"group = {args.group}")
    print(f"{'topic':<{w}}{'partition':>9}  {'current-offset':>14}  {'log-end-offset':>14}  {'lag':>6}")
    for t, p, cur, leo, lag in rows:
        print(f"{t:<{w}}{p:>9}  {cur:>14}  {leo:>14}  {lag:>6}")
    print(f"{'TOTAL LAG':<{w}}{'':>9}  {'':>14}  {'':>14}  {total:>6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

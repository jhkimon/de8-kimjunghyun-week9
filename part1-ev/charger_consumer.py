"""Q3 — Kafka Consumer 그룹 · 오프셋 직접 관리 · 리밸런싱

순수 파이썬 Kafka 컨슈머(그룹 ev-consumer-kjh)로 ev-charging-sessions 를 소비하며
endedBy 별 건수를 셉니다.

- 오프셋 직접 관리: enable.auto.commit=false. 메시지 1건의 처리(집계)를 마친 뒤에만
  그 메시지 오프셋을 동기 커밋 → 리밸런싱으로 파티션이 넘어가도 처리한 만큼만 반영되어
  새 소유자가 다음 오프셋부터 이어 받음 (유실·중복 집계 없음)
- 리밸런싱: on_assign / on_revoke 콜백으로 파티션 할당·회수를 출력

실행 (EC2 호스트)
  두 인스턴스 동시 실행 + 결과 모아 보기 : ~/venv/bin/python part1-ev/charger_consumer.py --spawn 2
  인스턴스 하나만 실행(터미널 2개에서 각각): ~/venv/bin/python part1-ev/charger_consumer.py --instance 1
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, TopicPartition

TOPIC = "ev-charging-sessions"
GROUP_ID = "ev-consumer-kjh"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092,localhost:39092,localhost:49092")


# ───────────────────────────── 컨슈머 인스턴스 ─────────────────────────────
def run_instance(instance: int, proc_delay: float) -> int:
    stop = {"flag": False}

    def _stop(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"charger-consumer-kjh-{instance}",
        "enable.auto.commit": False,          # 자동 커밋 미사용
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",      # 커밋 오프셋이 없으면 처음부터
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })

    counts: Counter = Counter()
    committed: dict[int, int] = {}            # 파티션 → 이 인스턴스가 마지막으로 커밋한 오프셋(다음 읽을 위치)

    def on_assign(c, partitions):
        if stop["flag"]:
            return
        print(f"[할당] {sorted(p.partition for p in partitions)}", flush=True)

    def on_revoke(c, partitions):
        # 처리 직후 매번 동기 커밋하므로 회수 시점에 미커밋 처리분이 없음
        if stop["flag"]:
            return
        print(f"[회수(리밸런싱)] {sorted(p.partition for p in partitions)}", flush=True)

    print(f"===== consumer #{instance} (pid {os.getpid()}) =====", flush=True)
    print(f"group_id={GROUP_ID} 소비 시작", flush=True)
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    idle_reported = False
    try:
        while not stop["flag"]:
            msg = consumer.poll(1.0)
            if msg is None:
                if counts and not idle_reported:
                    print(f"[대기] 새 메시지 없음 — 지금까지 처리 {sum(counts.values())}건", flush=True)
                    idle_reported = True
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[오류] {msg.error()}", file=sys.stderr, flush=True)
                continue

            # 1) 처리
            record = json.loads(msg.value())
            counts[record["endedBy"]] += 1
            time.sleep(proc_delay)            # 처리 시간 가정 (리밸런싱 관찰용)

            # 2) 처리를 마친 메시지만 오프셋 반영 (offset+1 을 동기 커밋)
            consumer.commit(message=msg, asynchronous=False)
            committed[msg.partition()] = msg.offset() + 1
            idle_reported = False
    finally:
        total = sum(counts.values())
        print(f"[종료] 처리 {total}건 / 커밋 오프셋 {dict(sorted(committed.items()))}")
        print("endedBy 별 건수:")
        for k, v in counts.most_common():
            print(f"  {k:<22}{v:>4}")
        print(f"RESULT {json.dumps(dict(counts), ensure_ascii=False)}", flush=True)
        consumer.close()                       # 그룹 탈퇴 → 남은 인스턴스로 리밸런싱
    return 0


# ───────────────────────── 2개 동시 실행 + 결과 취합 ─────────────────────────
def group_progress() -> tuple[int, int]:
    """(그룹 커밋 오프셋 합, 토픽 end offset 합). 그룹에 가입하지 않는 조회 전용 클라이언트."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP_ID, "enable.auto.commit": False})
    try:
        parts = [TopicPartition(TOPIC, p) for p in c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions]
        done = sum(max(tp.offset, 0) for tp in c.committed(parts, timeout=10))
        end = sum(c.get_watermark_offsets(tp, timeout=10)[1] for tp in parts)
        return done, end
    finally:
        c.close()


def spawn(n: int, proc_delay: float, linger: int) -> int:
    procs = [
        subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--instance", str(i), "--proc-delay", str(proc_delay)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        for i in range(1, n + 1)
    ]
    print(f"컨슈머 {n}개 동시 실행: pid {[p.pid for p in procs]}  group_id={GROUP_ID}", flush=True)
    try:
        while True:
            time.sleep(2)
            done, end = group_progress()
            if done >= end > 0:
                break
        print(f"그룹 커밋 오프셋 {done}/{end} — 처리 완료. {linger}초 뒤 종료 (이때 Kafka UI Consumers 캡처)", flush=True)
        time.sleep(linger)
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        outputs = [p.communicate(timeout=60)[0] for p in procs]

    total: Counter = Counter()
    for out in outputs:
        for line in out.splitlines():
            if line.startswith("RESULT "):
                total.update(json.loads(line[len("RESULT "):]))
            else:
                print(line)
    print("===== 두 인스턴스 합계 =====")
    for k, v in total.most_common():
        print(f"  {k:<22}{v:>4}")
    print(f"  {'합계':<20}{sum(total.values()):>4}  (토픽 전체 {end}건)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--instance", type=int, help="인스턴스 번호로 컨슈머 1개 실행")
    g.add_argument("--spawn", type=int, help="컨슈머 N개를 별도 프로세스로 동시 실행")
    ap.add_argument("--proc-delay", type=float, default=0.1, help="메시지당 처리 시간(초)")
    ap.add_argument("--linger", type=int, default=90, help="--spawn: 처리 완료 후 유지 시간(초)")
    args = ap.parse_args()
    if args.instance:
        return run_instance(args.instance, args.proc_delay)
    return spawn(args.spawn, args.proc_delay, args.linger)


if __name__ == "__main__":
    sys.exit(main())

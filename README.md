https://github.com/jhkimon/de8-kimjunghyun-week9

# 데엔8기 김정현 — 9주차 (Kafka on EC2 · S3 Bronze/Silver/Gold · Athena · Airflow)

| 항목 | 값 |
|---|---|
| 이니셜 | `kjh` |
| Kafka 컨슈머 그룹 | `ev-consumer-kjh` (Q3·Q10) |
| Athena 데이터베이스 | `de9_kjh` (Q5 `ev_charging_agg`, Q9 `grocery_price_change`) |
| Airflow DAG | `price_pipeline_kjh` (Q9) |
| S3 버킷 | `de9-kimjunghyun` (ap-northeast-2) |
| EC2 | t3.large · Ubuntu 22.04 · EBS 30GiB · IAM Role `de9-ec2-kjh` · IMDSv2 hop limit 2 |

AWS 접근은 전부 EC2 IAM Role 로만 합니다. 레포·compose·코드 어디에도 액세스 키가 없습니다.
(Slack 웹훅 URL 은 EC2 의 `part1-ev/.env` 에만 두고 `.gitignore` 로 제외)

## 구성 (`part1-ev/docker-compose.yaml`, EC2 한 대)

| 컨테이너 | 이미지 | 호스트 포트 |
|---|---|---|
| kafka1 / kafka2 / kafka3 | apache/kafka:3.7.0 (KRaft, broker+controller) | 29092 / 39092 / 49092 |
| kafka-ui | provectuslabs/kafka-ui:v0.7.2 | 8080 |
| spark-master | apache/spark:3.5.3 (Java 11 · Python 3.8) | 8081 (UI), 4040 |
| spark-worker | apache/spark:3.5.3 (2 core, 1.5GiB) | - |
| airflow | `part2-price/airflow/Dockerfile` (Airflow 2.10.5 · Python 3.8 · pyspark 3.5.3 · JRE) | 8090 |

## 구조

```
part1-ev/                       Part 1 전기차 충전 세션 (Q1~Q5)
  docker-compose.yaml           Q1 전체 스택
  requirements.txt              호스트 파이썬: confluent-kafka 2.16.2, kafka-python 3.0.13
  charger_producer.py           Q2 startTime 순 replay, acks=all + idempotence, key=stationName
  charger_consumer.py           Q3 그룹 ev-consumer-kjh, 수동 동기 커밋, 리밸런싱 콜백, --spawn 2
  charger_stream.py             Q4 Structured Streaming → bronze/silver + 설치구역 누적 집계
                                Q5 (같은 foreachBatch) idleSec>5400 → Slack, gold/ev_charging_agg
part2-price/                    Part 2 식료품 가격 배치 (Q6~Q10)
  airflow/Dockerfile            Spark 클러스터에 제출 가능한 Airflow 이미지
  price_producer.py             Q6 kafka-python, RegionPartitioner(북부 0 / 서부·중부 1 / 남부·동부 2)
  price_batch.py                Q7 배치 읽기 → bronze/silver + 품목·월 집계
                                Q8 윈도우 함수(lag, row_number) 변동률 순위 → gold/grocery_price_change
  dags/price_pipeline_dag.py    Q9 SparkSubmitOperator → Athena 테이블 등록 → Athena 조회(쿼리 ID XCom)
  kafka_lag_monitor.py          Q10 그룹 파티션별 current/log-end offset, lag, TOTAL LAG
data/                           제공 CSV (수정 없음)
part*/capture/Q*/               문항별 캡처
git_log.txt                     git log --stat
```

## 실행 (EC2 호스트, `~/de9`)

```bash
# Q1 스택 + 토픽
cd ~/de9/part1-ev && docker compose up -d --build && cd ~/de9
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --topic ev-charging-sessions --partitions 3 --replication-factor 2

# Q2 / Q3
~/venv/bin/python part1-ev/charger_producer.py
~/venv/bin/python part1-ev/charger_consumer.py --spawn 2

# Q4·Q5 (Slack 웹훅은 part1-ev/.env 의 SLACK_WEBHOOK_URL)
SUBMIT="/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 512m --executor-memory 1g --total-executor-cores 2"
docker exec --env-file part1-ev/.env spark-master $SUBMIT /opt/work/part1-ev/charger_stream.py

# Q6 / Q7·Q8
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --topic product-prices --partitions 3 --replication-factor 2
~/venv/bin/python part2-price/price_producer.py
docker exec spark-master $SUBMIT /opt/work/part2-price/price_batch.py

# Q9: Airflow(http://<EC2>:8090) 에서 price_pipeline_kjh unpause → */10 스케줄 2회 성공 후 pause

# Q10
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 \
  --group ev-consumer-kjh --reset-offsets --to-earliest --topic ev-charging-sessions --execute
~/venv/bin/python part2-price/kafka_lag_monitor.py ev-consumer-kjh
```

## 결과 요약 (제공 데이터와 일치)

| 문항 | 결과 |
|---|---|
| Q2 | 175건 전송 |
| Q3 | Plug Out at Vehicle 155 · Customer 18 · CPS Server 2 |
| Q4 | 설치구역 10곳 누적: HIGH 44 · WEBSTER 30 · MPL 22 · CAMBRIDGE 21 · BRYANT 18 · HAMILTON 16 · TED THOMPSON 11 · RINCONADA LIB 3/1/2 = 5/4/4 |
| Q5 | 포트 점유 이상 18건: CAMBRIDGE·MPL·WEBSTER 4 · HIGH 2 · BRYANT·HAMILTON·RINCONADA LIB 1·TED THOMPSON 1 |
| Q6 | partition 0/1/2 = 80/60/60 (합계 200) |
| Q7 | Bronze 200 · Silver 200 · 품목×월 20행 |
| Q8 | Gold 100행, 도시별 1위 10행 (Kanpur Potatoes −47.57 등) |
| Q9 | 스케줄 실행 2회 success, XCom 쿼리 ID = Athena 최근 쿼리 실행 ID |
| Q10 | 리셋 후 lag 76/35/64, TOTAL LAG 175 = 토픽 메시지 수 |

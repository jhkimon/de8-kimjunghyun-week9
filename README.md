https://github.com/jhkimon/de8-kimjunghyun-week9

# 데엔8기 김정현 — 9주차 (Kafka on EC2 · S3 Bronze/Silver/Gold · Athena)

- 본인 식별자(이니셜): `kjh`
  - Kafka 컨슈머 그룹 `ev-consumer-kjh` · Athena DB `de9_kjh` · Airflow DAG `price_pipeline_kjh` · EC2 IAM Role `de9-ec2-kjh`
- S3 버킷: `de9-kimjunghyun` (ap-northeast-2)
- 실행 환경: AWS EC2 t3.large(Ubuntu 22.04, EBS 30GiB) 1대, IAM Role 로만 S3·Athena 접근 (액세스 키 없음)

## 구성 (part1-ev/docker-compose.yaml)

| 컨테이너 | 이미지 | 호스트 포트 |
|---|---|---|
| kafka1 / kafka2 / kafka3 | apache/kafka:3.7.0 (KRaft, broker+controller, heap 384MiB) | 29092 / 39092 / 49092 |
| kafka-ui | provectuslabs/kafka-ui:v0.7.2 | 8080 |
| spark-master | apache/spark:3.5.3 | 8081 (UI), 4040 (app UI) |
| spark-worker | apache/spark:3.5.3 (2 core, 1.5GiB) | - |
| airflow | de9-airflow:2.10.5 (part2-price/airflow/Dockerfile) | 8090 |

## 실행

```bash
git clone https://github.com/jhkimon/de8-kimjunghyun-week9.git ~/de9 && cd ~/de9/part1-ev
docker compose up -d --build
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --topic ev-charging-sessions --partitions 3 --replication-factor 2
```

## 구조

```
part1-ev/      Part 1 전기차 충전 세션 (Q1~Q5)
part2-price/   Part 2 식료품 가격 배치 (Q6~Q10), airflow/Dockerfile, dags/
data/          제공 CSV (수정 금지)
```

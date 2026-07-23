# Docker Compose 3노드 KRaft 예제 (PLAINTEXT)

신뢰된 사설망(또는 학습·검증)에서 인증·암호화 없이 빠르게 올리는 3노드 KRaft 예제입니다. 메타데이터는 KRaft(combined)이고, 무손실 설정(RF3/minISR2 등)은 그대로 포함합니다.

> 운영처럼 서비스 계정 인증과 전송 암호화가 필요하면 **SASL_SSL + mTLS 버전**을 쓰세요 → [../compose-3node-kraft](../compose-3node-kraft/README.md). 두 예제의 차이는 **보안뿐**이고, 둘 다 KRaft입니다.

설계·절차 배경: [06-cluster-design](../../concepts/06-cluster-design.md), [07-kraft-cluster-installation](../../concepts/07-kraft-cluster-installation.md).

## 구성 파일

```text
docker-compose.yml   3대 공용 (노드 차이는 .env 로만 주입)
.env.example         노드별로 .env 로 복사해 NODE_ID/IP/CLUSTER_ID 지정
```

## 실행 순서

전제: 3대 = kafka1(10.0.0.11), kafka2(10.0.0.12), kafka3(10.0.0.13). 실제 값으로 바꿔 사용합니다.

1. **데이터 디렉터리 (3대 각각)**

   ```bash
   sudo mkdir -p /data/kafka && sudo chown -R 1000:1000 /data/kafka
   ```

2. **클러스터 ID 생성 (한 번, 3대 공유)**

   ```bash
   docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid
   # 출력값을 3대 .env 의 CLUSTER_ID 에 동일하게 넣는다
   ```

3. **노드별 .env 작성**

   ```bash
   cp .env.example .env
   # 서버1: KAFKA_NODE_ID=1, ADVERTISED_HOST=10.0.0.11, CLUSTER_ID=<생성값>
   # 서버2/3 도 각자 값으로
   ```

4. **기동 (3대 각각)** — CLUSTER_ID 가 있으면 이미지가 최초 기동 시 스토리지를 자동 포맷합니다.

   ```bash
   docker compose up -d
   docker compose logs -f kafka   # "Kafka Server started" 확인
   ```

5. **확인**

   ```bash
   # 컨트롤러 쿼럼 (voters 3, leader)
   docker compose exec kafka /opt/kafka/bin/kafka-metadata-quorum.sh \
     --bootstrap-controller 10.0.0.11:9093 describe --status

   # 브로커 3개 확인
   docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
     --bootstrap-server 10.0.0.11:9092

   # RF3 토픽 생성 + 분포 확인
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server 10.0.0.11:9092 \
     --create --topic order-created --partitions 3 --replication-factor 3 \
     --config min.insync.replicas=2
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server 10.0.0.11:9092 --describe --topic order-created
   #   → Leader/Replicas/Isr 에 1,2,3 이 보이면 정상
   ```

## 방화벽 (3대 각각, RHEL/firewalld)

```bash
sudo firewall-cmd --permanent --add-port=9092/tcp --add-port=9093/tcp   # 신뢰 대역만 권장
sudo firewall-cmd --reload
```

## 앱 접속

```text
bootstrap.servers = 10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092
+ producer: acks=all, enable.idempotence=true   ← 무손실은 이게 있어야 완성
```

## 주의

- **PLAINTEXT 는 인증·암호화가 없습니다.** 신뢰된 내부망 전제이며, 외부에 포트를 열지 마세요.
- `.env` 는 커밋하지 않습니다(루트 `.gitignore` 대상).
- `CLUSTER_ID`·`KAFKA_CONTROLLER_QUORUM_VOTERS` 는 3대 동일해야 하나로 묶입니다.

# Kafka 운영 명령어 치트시트

3노드 KRaft(SASL_SSL) 클러스터를 기준으로 자주 쓰는 명령을 모읍니다. 배경은 [설치 문서](../concepts/03-kraft-cluster-installation.md)를 참고합니다.

명령은 컨테이너 안에서 실행하는 형태(`docker compose exec kafka ...`)로 적습니다. 모든 CLI 는 보안 리스너에 접속하므로 `--command-config` 로 클라이언트 설정 파일이 필요합니다.

## 클라이언트 설정 파일 (SASL_SSL)

앱과 CLI 가 공통으로 쓰는 접속 설정입니다. `client-sasl-ssl.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="order-service" password="__SET_ME__";
ssl.truststore.location=/etc/kafka/secrets/truststore.jks
ssl.truststore.password=__SET_ME__
ssl.endpoint.identification.algorithm=https
```

컨트롤러 접속용(`client-ssl.properties`)은 mTLS 이므로 keystore 도 포함합니다:

```properties
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/truststore.jks
ssl.truststore.password=__SET_ME__
ssl.keystore.location=/etc/kafka/secrets/kafka1.keystore.jks
ssl.keystore.password=__SET_ME__
```

## 클러스터/쿼럼 상태

```bash
# controller quorum 상태 (leader, voters 3 확인)
kafka-metadata-quorum.sh --bootstrap-controller kafka1:9093 \
  --command-config /etc/kafka/client-ssl.properties describe --status

# broker API 버전 (클라이언트 리스너 접속 확인)
kafka-broker-api-versions.sh --bootstrap-server kafka1:9094 \
  --command-config /etc/kafka/client-sasl-ssl.properties
```

## 토픽

```bash
BS=kafka1:9094
CFG=/etc/kafka/client-sasl-ssl.properties

# 목록
kafka-topics.sh --bootstrap-server $BS --command-config $CFG --list

# 생성 (RF3, minISR2 명시)
kafka-topics.sh --bootstrap-server $BS --command-config $CFG \
  --create --topic order-created --partitions 6 \
  --replication-factor 3 --config min.insync.replicas=2

# 상세 (Leader/Replicas/ISR 확인 — 장애 조치 판단의 기준)
kafka-topics.sh --bootstrap-server $BS --command-config $CFG \
  --describe --topic order-created

# 파티션 증설 (줄일 수는 없음)
kafka-topics.sh --bootstrap-server $BS --command-config $CFG \
  --alter --topic order-created --partitions 12
```

## 컨슈머 그룹

```bash
# 그룹 목록
kafka-consumer-groups.sh --bootstrap-server $BS --command-config $CFG --list

# 랙(lag) 확인 — 소비 지연 모니터링의 핵심
kafka-consumer-groups.sh --bootstrap-server $BS --command-config $CFG \
  --describe --group order-service

# 오프셋 리셋 (그룹 정지 상태에서만)
kafka-consumer-groups.sh --bootstrap-server $BS --command-config $CFG \
  --group order-service --topic order-created \
  --reset-offsets --to-earliest --execute
```

## 계정(SCRAM)과 ACL

```bash
# 서비스 계정 생성/변경
kafka-configs.sh --bootstrap-server $BS --command-config $CFG \
  --alter --add-config 'SCRAM-SHA-512=[password=__SET_ME__]' \
  --entity-type users --entity-name order-service

# 계정에 토픽 권한 부여 (특정 토픽만 read/write)
kafka-acls.sh --bootstrap-server $BS --command-config $CFG \
  --add --allow-principal User:order-service \
  --operation Read --operation Write --topic order-created
```

## 스모크 테스트 (produce/consume)

```bash
# 프로듀서 (acks=all)
kafka-console-producer.sh --bootstrap-server $BS \
  --producer.config $CFG --topic order-created --producer-property acks=all

# 컨슈머
kafka-console-consumer.sh --bootstrap-server $BS \
  --consumer.config $CFG --topic order-created --from-beginning
```

## 장애 조치/점검 빠른 참조

```bash
# 특정 토픽 ISR 이 RF 와 같은지 (건강 상태)
kafka-topics.sh --bootstrap-server $BS --command-config $CFG --describe --topic order-created

# under-replicated 파티션 전체 스캔 (복제 뒤처짐 탐지)
kafka-topics.sh --bootstrap-server $BS --command-config $CFG \
  --describe --under-replicated-partitions
```

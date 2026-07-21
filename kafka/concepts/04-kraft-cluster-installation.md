# KRaft 3노드 클러스터 설치 및 설정 방법

이 문서는 [실서버 3대 클러스터 설계 방식](03-cluster-design.md)에서 정한 구성(KRaft combined 3노드, RF3/minISR2, SASL_SSL + mTLS)을 **RHEL/Rocky/CentOS 계열에서 Docker Compose로** 설치하는 절차를 정리합니다.

실행 가능한 설정 파일은 [`examples/compose-3node-kraft/`](../examples/compose-3node-kraft/README.md)에 있습니다. 이 문서는 그 파일들을 어떤 순서로, 왜 그렇게 쓰는지 설명합니다.

전제: 서버 3대를 각각 `kafka1`, `kafka2`, `kafka3` (실 IP `10.0.0.11/12/13` 예시)로 부릅니다.

## 1. 언제 이 구성을 선택하는가

좋은 경우:

- 노드 3대로 최소 HA를 만들고 싶은 MSA 메시징 백본.
- ZooKeeper 없이 운영을 단순화하고 싶은 신규 구축.
- 서비스 계정 인증과 전송 암호화가 필요한 사내 프로덕션.

이럴 땐 다시 검토:

- 데이터 유실이 허용되고 단일 장애점이 무방한 개발용이면 → 단일 broker로 충분(이 문서는 과함).
- broker 트래픽이 매우 커 controller와 자원 경합이 예상되면 → controller 분리(dedicated) 검토.

## 2. 최소 구성과 권장 구성

```text
[권장: 실서버 3대]
  kafka1(node.id=1)   kafka2(node.id=2)   kafka3(node.id=3)
  broker+controller   broker+controller   broker+controller
  → controller quorum 3표 = 1대 장애 허용, RF3 저장 가능

[학습/검증: 단일 머신 3컨테이너]
  한 호스트에 kafka1/2/3 컨테이너를 포트만 달리해 기동 (운영용 아님)
```

이 문서는 권장 구성을 기준으로 하고, 마지막에 단일 머신 검증 구성을 따로 둡니다.

## 3. 네트워크 요구사항

| 포트 | 리스너 | 방향 | 개방 범위 |
| --- | --- | --- | --- |
| 9092 | INTERNAL (브로커 간, SASL_SSL) | 노드 ↔ 노드 | 3노드 사이에서만 |
| 9093 | CONTROLLER (쿼럼, SSL/mTLS) | 노드 ↔ 노드 | 3노드 사이에서만 |
| 9094 | CLIENT (앱 접속, SASL_SSL) | 앱 → 노드 | 애플리케이션 대역 |

firewalld 예시 (각 노드에서, 신뢰 대역만 허용):

```bash
# 3노드 사이 내부 통신 (예: 10.0.0.0/29 안에 3대가 있다고 가정)
sudo firewall-cmd --permanent --zone=internal --add-source=10.0.0.11/32
sudo firewall-cmd --permanent --zone=internal --add-source=10.0.0.12/32
sudo firewall-cmd --permanent --zone=internal --add-source=10.0.0.13/32
sudo firewall-cmd --permanent --zone=internal --add-port=9092/tcp
sudo firewall-cmd --permanent --zone=internal --add-port=9093/tcp

# 애플리케이션 대역에는 클라이언트 포트만 (예: 앱 서버 대역 10.0.1.0/24)
sudo firewall-cmd --permanent --zone=internal --add-source=10.0.1.0/24
sudo firewall-cmd --permanent --zone=internal --add-port=9094/tcp

sudo firewall-cmd --reload
```

## 4. 설치 전 체크리스트

- [ ] Docker / Docker Compose plugin 설치 (`docker --version`, `docker compose version`)
- [ ] 데이터용 전용 디스크를 `/data/kafka`에 마운트하고 권한 부여
- [ ] 3대 시간 동기화 (`chronyd` 활성화) — 시계 어긋나면 TLS/타임스탬프 문제 발생
- [ ] 3대가 서로의 호스트명/IP를 해석 가능 (`/etc/hosts` 또는 DNS)
- [ ] 파일 디스크립터 상한 상향 (`ulimit -n` 100000 권장)
- [ ] 방화벽 규칙 적용(3장) 및 노드 간 9092/9093 연결 확인

```bash
sudo mkdir -p /data/kafka
# apache/kafka 이미지의 기본 실행 UID(1000)에 쓰기 권한 부여
sudo chown -R 1000:1000 /data/kafka
```

## 5. TLS 인증서 준비

사설 CA 1개를 만들고, 노드마다 keystore(자기 인증서)와 truststore(CA 신뢰)를 발급합니다. 전체 스크립트는 [`certs/README.md`](../examples/compose-3node-kraft/certs/README.md)에 있고, 여기서는 흐름만 정리합니다.

```text
사설 CA (ca.crt / ca.key)
  ├─ kafka1: kafka1.keystore.jks  (CN/SAN = kafka1, 10.0.0.11)
  ├─ kafka2: kafka2.keystore.jks  (CN/SAN = kafka2, 10.0.0.12)
  ├─ kafka3: kafka3.keystore.jks  (CN/SAN = kafka3, 10.0.0.13)
  └─ 공통 truststore.jks (CA 인증서 포함)  ← 모든 노드/클라이언트가 신뢰
```

중요한 점:

- 각 노드 인증서의 **SAN(Subject Alternative Name)에 그 노드의 호스트명과 IP를 모두** 넣습니다. `advertised.listeners`에 쓰는 주소와 일치해야 hostname verification을 통과합니다.
- 컨트롤러 리스너는 mTLS이므로 노드 인증서가 **서버이자 클라이언트**로 쓰입니다. `extendedKeyUsage`에 serverAuth와 clientAuth를 모두 넣습니다.
- keystore/truststore 비밀번호는 `.env`가 아니라 배포 시 Secret Manager로 주입합니다(예제에서는 `.env.example`에 자리만 표시).

## 6. docker-compose.yml 핵심 설정

3대가 **같은 compose 파일**을 쓰고, 노드별 차이는 `.env`로만 주입합니다. 전체 파일은 예제 폴더에 있고, 핵심 env만 설명합니다.

```yaml
environment:
  # --- KRaft 역할/식별 ---
  KAFKA_PROCESS_ROLES: broker,controller
  KAFKA_NODE_ID: ${KAFKA_NODE_ID}                 # 노드별 1/2/3
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093

  # --- 리스너 정의 ---
  KAFKA_LISTENERS: INTERNAL://:9092,CONTROLLER://:9093,CLIENT://:9094
  KAFKA_ADVERTISED_LISTENERS: INTERNAL://${ADVERTISED_HOST}:9092,CLIENT://${ADVERTISED_HOST}:9094
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:SASL_SSL,CONTROLLER:SSL,CLIENT:SASL_SSL
  KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

  # --- 무손실 기본값 (02 문서 표) ---
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  # --- 저장 위치 ---
  KAFKA_LOG_DIRS: /var/lib/kafka/data
```

중요한 설정:

- `KAFKA_PROCESS_ROLES=broker,controller`: 이 노드가 broker와 controller를 겸합니다(combined 모드).
- `KAFKA_CONTROLLER_QUORUM_VOTERS`: **3대 모두 동일**하게 세 노드를 나열합니다. `id@host:9093` 형식.
- `KAFKA_ADVERTISED_LISTENERS`: 클라이언트/다른 broker가 접속할 **실제 주소**. `${ADVERTISED_HOST}`에 그 노드의 실 IP/호스트명을 넣습니다. CONTROLLER는 advertised에 넣지 않습니다(quorum voters로 알림).
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`: 리스너별 보안 프로토콜. CONTROLLER는 SSL(mTLS), 나머지는 SASL_SSL.
- 무손실 관련 값은 [03 문서](03-cluster-design.md)의 표를 그대로 broker 기본값으로 넣은 것입니다.

SASL/SCRAM·TLS 관련 env(SCRAM JAAS, keystore/truststore 경로·비밀번호)와 볼륨 마운트는 예제 compose에 포함되어 있습니다.

## 7. 클러스터 ID 생성과 스토리지 포맷

KRaft는 **최초 1회** 클러스터 ID를 만들고 각 노드의 로그 디렉터리를 포맷해야 합니다.

```bash
# (1) 클러스터 ID 한 번만 생성 → 3대가 같은 값을 공유
KAFKA_CLUSTER_ID=$(docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid)
echo "$KAFKA_CLUSTER_ID"   # 예: xtzWWN4bTjitpL3kfd9s5g

# (2) 각 노드에서 스토리지 포맷 (SCRAM 관리자 계정도 함께 부트스트랩)
#     --add-scram 으로 broker 간/관리용 SCRAM 계정을 메타데이터에 심는다
docker compose run --rm kafka \
  /opt/kafka/bin/kafka-storage.sh format \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --config /etc/kafka/server.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=CHANGE_ME_ADMIN]'
```

중요한 점:

- 클러스터 ID는 **3대가 동일**해야 합니다. 한 번 생성해 세 노드에 같은 값으로 전달합니다.
- 포맷은 빈 로그 디렉터리에서 최초 1회만 합니다. 이미 데이터가 있는 디렉터리를 다시 포맷하면 지워집니다.
- `--add-scram`으로 부트스트랩한 admin 계정은 이후 다른 서비스 계정을 만들 때 씁니다.

## 8. 기동과 검증

세 노드에서 순서대로 기동합니다(순서는 크게 중요치 않지만, 하나씩 올리며 로그를 확인합니다).

```bash
# 각 노드에서
docker compose up -d
docker compose logs -f kafka   # "Kafka Server started" 확인
```

controller quorum과 broker 상태 확인:

```bash
# controller quorum 상태 (leader와 voters 3 확인)
docker compose exec kafka /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller kafka1:9093 \
  --command-config /etc/kafka/client-ssl.properties \
  describe --status

# broker API 확인 (클라이언트 리스너로 접속)
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka1:9094 \
  --command-config /etc/kafka/client-sasl-ssl.properties
```

## 9. SCRAM 서비스 계정 생성

앱마다 별도 SCRAM 계정을 만들어 권한을 분리합니다.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:9094 \
  --command-config /etc/kafka/client-sasl-ssl.properties \
  --alter --add-config 'SCRAM-SHA-512=[password=CHANGE_ME_ORDERSVC]' \
  --entity-type users --entity-name order-service
```

주의할 점:

- 비밀번호는 명령 히스토리·로그에 남지 않도록 주입 방식을 관리합니다(파일/Secret Manager).
- 더 세밀히 제한하려면 이 계정에 ACL을 부여해 특정 토픽만 read/write 하도록 합니다.

## 10. 토픽 생성과 스모크 테스트

```bash
# RF3, min.insync.replicas=2 를 토픽 단위로 명시
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9094 \
  --command-config /etc/kafka/client-sasl-ssl.properties \
  --create --topic order-created --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# 프로듀서 (acks=all)
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:9094 \
  --producer.config /etc/kafka/client-sasl-ssl.properties \
  --topic order-created --producer-property acks=all

# 컨슈머
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka2:9094 \
  --consumer.config /etc/kafka/client-sasl-ssl.properties \
  --topic order-created --from-beginning
```

## 11. 장애 조치 테스트와 롤링 재시작

장애 조치 확인 (무손실·무중단):

```bash
# 1) 토픽 상태에서 Leader/ISR 확인
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9094 --command-config /etc/kafka/client-sasl-ssl.properties \
  --describe --topic order-created

# 2) 한 노드(예: kafka3) 정지
#    kafka3에서:
docker compose stop kafka

# 3) 다른 노드에서 produce/consume 이 계속되는지, ISR이 2로 줄었는지 확인
#    → RF3 + minISR2 이므로 쓰기가 계속되어야 정상

# 4) kafka3 복구 후 ISR이 다시 3으로 회복되는지 확인
docker compose start kafka
```

롤링 재시작(설정 변경/업그레이드 시): 한 번에 **한 노드씩** 재시작하고, ISR이 3으로 회복된 뒤 다음 노드로 넘어갑니다. controller leader 노드는 마지막에 재시작합니다.

## 12. 자주 만나는 문제

### 클라이언트가 붙었다가 다른 주소로 재접속하며 실패

- 원인 후보: `advertised.listeners`가 컨테이너 내부 호스트명이나 잘못된 IP로 설정됨.
- 확인: 클라이언트 로그에서 재접속 대상 주소를 확인. `kafka-broker-api-versions.sh` 결과의 광고 주소 확인.
- 해결: `${ADVERTISED_HOST}`를 각 노드의 **실 IP/호스트명**으로 지정. 인증서 SAN에도 같은 값 포함.

### controller quorum이 형성되지 않음 / "no leader"

- 원인 후보: `KAFKA_CONTROLLER_QUORUM_VOTERS`가 노드마다 다르거나, 9093이 방화벽에 막힘, 클러스터 ID 불일치.
- 확인: `kafka-metadata-quorum.sh ... describe --status`의 voters·leader. 노드 간 `nc -vz kafka2 9093`.
- 해결: voters 문자열을 3대 동일하게, 9093 개방, 3대 동일 클러스터 ID로 포맷.

### TLS handshake 실패 (SSLHandshakeException)

- 원인 후보: truststore에 CA 미포함, 인증서 SAN 불일치, keystore/truststore 비밀번호 오류, 시계 어긋남.
- 확인: broker 로그의 handshake 오류 메시지, `openssl s_client -connect kafka1:9094`로 인증서 체인 확인.
- 해결: 공통 truststore에 CA 포함, SAN에 접속 주소 포함, chrony로 시간 동기화.

### 쓰기가 갑자기 거부됨 (NotEnoughReplicas)

- 원인 후보: 2대 이상 다운되어 ISR이 `min.insync.replicas` 미만.
- 확인: `kafka-topics.sh --describe`의 ISR 개수.
- 해결: 다운된 노드 복구. 이 거부는 무손실을 위한 **의도된 동작**이므로 minISR을 낮춰 회피하지 않습니다.

## 13. 운영 전 최종 점검

- [ ] 3대에서 `describe --status`의 voters=3, leader 존재
- [ ] 테스트 토픽 RF=3, ISR=3 확인
- [ ] 1대 정지 후에도 produce/consume 지속(무중단), 복구 후 ISR 3 회복
- [ ] 9092/9093은 3노드 내부, 9094만 앱 대역에 개방됨
- [ ] SASL_SSL로만 접속 가능(평문 접속 거부), 잘못된 계정 인증 거부
- [ ] 시크릿이 파일에 하드코딩되지 않고 Secret Manager로 주입됨
- [ ] `/data/kafka`가 전용 디스크이고 컨테이너 재생성 후에도 데이터 보존

## 14. 참고한 공식 문서

- [KRaft Configuration](https://kafka.apache.org/documentation/#kraft_config)
- [Running Kafka in KRaft mode / storage format](https://kafka.apache.org/documentation/#kraft_storage)
- [Security — SSL & SASL/SCRAM](https://kafka.apache.org/documentation/#security)
- [Adding SCRAM credentials](https://kafka.apache.org/documentation/#security_sasl_scram_credentials)
- [Apache Kafka Docker image](https://hub.docker.com/r/apache/kafka)

#!/usr/bin/env bash
#
# Kafka 홈랩 원클릭 구축 스크립트 (단일 노드)
#
# 사내 클러스터와 같은 보안 흐름을 집 PC 한 대에 재현한다:
#   - KRaft combined 1노드 (SASL_SSL + 컨트롤러 mTLS)
#   - StandardAuthorizer 인가 + super.users
#   - admin / schema-registry / app 계정(SCRAM) + ACL
#   - Schema Registry 1대 (_schemas compact 토픽)
#   - 실습용 sandbox.demo 토픽
#
# 브로커와 Schema Registry 는 운영처럼 별도 compose 프로젝트로 나눈다:
#   <작업디렉터리>/kafka/            브로커 (네트워크 kafka-home-lab-net 을 소유)
#   <작업디렉터리>/schema-registry/  SR (external 네트워크로 접속, depends_on 없음)
# 한쪽을 down 해도 다른 쪽은 살아 있다. 브로커 없이 SR 을 기동하면 restart 정책으로 반복
# 재시도하다 브로커가 뜨면 정상화되고, 이미 떠 있던 SR 은 브로커가 멈춰도 재연결만 한다.
#
# 요구사항: docker (compose v2 포함), openssl, bash
# 사용법:   ./setup.sh [작업디렉터리]      # 기본 ~/kafka-home-lab
# 정리:     cd <작업디렉터리> && docker compose --project-directory schema-registry down
#           && docker compose --project-directory kafka down -v
#
set -euo pipefail

LAB_DIR="${1:-$HOME/kafka-home-lab}"
SECRETS_DIR="$LAB_DIR/secrets"
DATA_VOLUME="kafka-home-lab-data"
KAFKA_IMAGE="apache/kafka:4.0.0"
SR_IMAGE="confluentinc/cp-schema-registry:7.7.0"
KAFKA_PROJECT="$LAB_DIR/kafka"
SR_PROJECT="$LAB_DIR/schema-registry"
# 각 프로젝트 디렉터리의 docker-compose.yml 과 .env 를 쓴다 (cd 없이 실행)
KAFKA_DC="docker compose --project-directory $KAFKA_PROJECT"
SR_DC="docker compose --project-directory $SR_PROJECT"

say()  { printf '\n\033[1;34m==> %s\033[0m\n' "$1"; }
fail() { printf '\033[1;31m오류: %s\033[0m\n' "$1" >&2; exit 1; }

# ---------- 0. 사전 점검 ----------
say "0/7 사전 점검"
command -v docker  >/dev/null || fail "docker 가 필요합니다"
command -v openssl >/dev/null || fail "openssl 이 필요합니다"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 가 필요합니다"
docker info >/dev/null 2>&1 || fail "docker 데몬이 실행 중이 아닙니다"

mkdir -p "$SECRETS_DIR"
cd "$LAB_DIR"

# ---------- 1. 비밀번호 생성 (.env — 재실행 시 기존 값 재사용) ----------
say "1/7 계정 비밀번호·클러스터 ID 준비"
if [ -f .env ]; then
  echo "기존 .env 재사용: $LAB_DIR/.env"
  # shellcheck disable=SC1091
  set -a; . ./.env; set +a
else
  ADMIN_PASSWORD=$(openssl rand -hex 16)
  SR_PASSWORD=$(openssl rand -hex 16)
  APP_PASSWORD=$(openssl rand -hex 16)
  STORE_PASSWORD=$(openssl rand -hex 16)
  KAFKA_CLUSTER_ID=$(docker run --rm "$KAFKA_IMAGE" /opt/kafka/bin/kafka-storage.sh random-uuid)
  cat > .env <<EOF
# kafka-home-lab 자격증명 — 커밋 금지
KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID
ADMIN_PASSWORD=$ADMIN_PASSWORD
SR_PASSWORD=$SR_PASSWORD
APP_PASSWORD=$APP_PASSWORD
STORE_PASSWORD=$STORE_PASSWORD
EOF
  chmod 600 .env
fi

# ---------- 2. 인증서 (사설 CA + PKCS12 keystore + PEM truststore — keytool 없이 openssl 만 사용) ----------
say "2/7 인증서 생성"
if [ -f "$SECRETS_DIR/kafka.keystore.p12" ]; then
  echo "기존 인증서 재사용"
else
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "$SECRETS_DIR/ca.key" -out "$SECRETS_DIR/ca.crt" \
    -subj "/CN=kafka-home-lab-CA" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$SECRETS_DIR/kafka.key" -out "$SECRETS_DIR/kafka.csr" \
    -subj "/CN=localhost" 2>/dev/null
  # SAN: 호스트에서는 localhost, 컨테이너 네트워크에서는 kafka 로 접속하므로 둘 다 넣는다.
  # EKU clientAuth: 컨트롤러 mTLS 에서 브로커가 TLS 클라이언트 역할도 하기 때문.
  openssl x509 -req -in "$SECRETS_DIR/kafka.csr" \
    -CA "$SECRETS_DIR/ca.crt" -CAkey "$SECRETS_DIR/ca.key" -CAcreateserial \
    -days 3650 -out "$SECRETS_DIR/kafka.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth") 2>/dev/null
  openssl pkcs12 -export -in "$SECRETS_DIR/kafka.crt" -inkey "$SECRETS_DIR/kafka.key" \
    -certfile "$SECRETS_DIR/ca.crt" -name kafka \
    -out "$SECRETS_DIR/kafka.keystore.p12" -passout "pass:$STORE_PASSWORD"
  # truststore 는 PKCS12 로 만들지 않고 ca.crt(PEM) 를 그대로 쓴다 (ssl.truststore.type=PEM).
  # openssl 이 만든 "인증서만 있는" PKCS12 에는 Java 가 신뢰 항목으로 인식하는 데 필요한
  # trusted-key-usage 속성이 없어 keytool 기준 0 entries 가 되고, 브로커가
  # "the trustAnchors parameter must be non-empty" 로 기동에 실패한다. (OpenSSL 3.2+ 의
  # -jdktrust 옵션으로도 해결되지만 macOS 기본 LibreSSL 에는 없어 PEM 이 더 안전하다.)
  rm -f "$SECRETS_DIR/kafka.csr"
fi

# ---------- 3. 클라이언트 접속 파일 (admin / app) ----------
say "3/7 접속 파일 생성"
for acct in admin app; do
  pw_var=$([ "$acct" = admin ] && echo "$ADMIN_PASSWORD" || echo "$APP_PASSWORD")
  cat > "$SECRETS_DIR/$acct.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$acct" password="$pw_var";
ssl.truststore.location=/etc/kafka/secrets/ca.crt
ssl.truststore.type=PEM
EOF
done
# 스토리지 포맷용 최소 설정 (포맷에만 사용 — 실행 시 설정은 compose 의 env 가 담당)
cat > "$SECRETS_DIR/format.properties" <<'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=/var/lib/kafka/data
EOF
# 컨테이너(UID 1000)가 읽을 수 있어야 한다 — 홈랩이므로 읽기 권한 완화
chmod -R a+r "$SECRETS_DIR"

# ---------- 4. docker-compose.yml (브로커 / Schema Registry 프로젝트 분리) ----------
say "4/7 docker-compose.yml 생성 (kafka/, schema-registry/)"
mkdir -p "$KAFKA_PROJECT" "$SR_PROJECT"

# 프로젝트별 .env — 운영처럼 각 서버가 자기 비밀만 갖는다 (SR 쪽에는 admin 비밀번호 없음)
cat > "$KAFKA_PROJECT/.env" <<EOF
KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID
ADMIN_PASSWORD=$ADMIN_PASSWORD
STORE_PASSWORD=$STORE_PASSWORD
EOF
cat > "$SR_PROJECT/.env" <<EOF
SR_PASSWORD=$SR_PASSWORD
EOF
chmod 600 "$KAFKA_PROJECT/.env" "$SR_PROJECT/.env"

cat > "$KAFKA_PROJECT/docker-compose.yml" <<'EOF'
# kafka-home-lab / 브로커 프로젝트 — setup.sh 가 생성. 사내 1노드 예제와 같은 구조에
# 인가(StandardAuthorizer)를 더한 구성. 운영의 /opt/kafka 에 해당한다.
# 이 프로젝트가 kafka-home-lab-net 네트워크를 만들고, Schema Registry 프로젝트가 여기에 붙는다.
name: kafka-home-lab
services:
  kafka:
    image: apache/kafka:4.0.0
    container_name: kafka-home-lab
    restart: unless-stopped
    networks: [kafka-home-lab-net]
    ports:
      - "9094:9094"                 # 호스트 앱 접속 (SASL_SSL)
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      CLUSTER_ID: ${KAFKA_CLUSTER_ID}

      # 리스너 4개: INTERNAL(브로커간 자리), CONTROLLER(mTLS), CLIENT(호스트),
      # DOCKER(kafka-home-lab-net 에 붙은 Schema Registry 등 컨테이너용 — kafka:9095 로 광고.
      #        다른 compose 프로젝트라도 같은 네트워크면 서비스명 kafka 가 DNS 로 풀린다)
      KAFKA_LISTENERS: INTERNAL://:9092,CONTROLLER://:9093,CLIENT://:9094,DOCKER://:9095
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://localhost:9092,CLIENT://localhost:9094,DOCKER://kafka:9095
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:SASL_SSL,CONTROLLER:SSL,CLIENT:SASL_SSL,DOCKER:SASL_SSL
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
      KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-512
      KAFKA_LISTENER_NAME_INTERNAL_SCRAM___SHA___512_SASL_JAAS_CONFIG: >-
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="admin" password="${ADMIN_PASSWORD}";
      KAFKA_LISTENER_NAME_CLIENT_SCRAM___SHA___512_SASL_JAAS_CONFIG: >-
        org.apache.kafka.common.security.scram.ScramLoginModule required;
      KAFKA_LISTENER_NAME_DOCKER_SCRAM___SHA___512_SASL_JAAS_CONFIG: >-
        org.apache.kafka.common.security.scram.ScramLoginModule required;

      KAFKA_SSL_KEYSTORE_LOCATION: /etc/kafka/secrets/kafka.keystore.p12
      KAFKA_SSL_KEYSTORE_TYPE: PKCS12
      KAFKA_SSL_KEYSTORE_PASSWORD: ${STORE_PASSWORD}
      KAFKA_SSL_KEY_PASSWORD: ${STORE_PASSWORD}
      # truststore 는 CA 의 PEM 파일을 직접 지정 (PEM 은 비밀번호 없음)
      KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/ca.crt
      KAFKA_SSL_TRUSTSTORE_TYPE: PEM
      KAFKA_LISTENER_NAME_CONTROLLER_SSL_CLIENT_AUTH: required
      KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: https

      # 인가 — 사내 구성과 동일. 브로커 인증서 주체(CN=localhost)를 super 에 포함해야
      # 컨트롤러(mTLS) 요청이 인가 준비 전에도 통과한다 (super.users 오타 주의: 복수형!)
      KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
      KAFKA_SUPER_USERS: User:admin;User:CN=localhost

      # 단일 노드 — 복제 계수 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
      - ../secrets:/etc/kafka/secrets:ro

networks:
  kafka-home-lab-net:
    name: kafka-home-lab-net

volumes:
  kafka-data:
    name: kafka-home-lab-data
EOF

cat > "$SR_PROJECT/docker-compose.yml" <<'EOF'
# kafka-home-lab / Schema Registry 프로젝트 — setup.sh 가 생성. 운영의 /opt/kafka-ecosystem 에 해당.
# 브로커 프로젝트와 분리되어 있어 depends_on 을 쓸 수 없다. 브로커 없이 SR 을 기동하면
# 이미지 진입점의 kafka-ready 확인에서 바로 종료하고 restart 정책으로 반복 재시도하다가
# 브로커가 뜨면 정상화된다 (실측: 브로커 기동 후 30초 내 복구). 이미 떠 있던 SR 은 브로커가
# 멈춰도 재시작하지 않고 재연결만 한다. setup.sh 는 브로커 → ACL → SR 순서로 올린다.
name: kafka-home-lab-sr
services:
  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.0
    container_name: sr-home-lab
    restart: unless-stopped
    networks: [kafka-home-lab-net]
    ports:
      - "8081:8081"
    volumes:
      - ../secrets:/etc/schema-registry/secrets:ro
    environment:
      SCHEMA_REGISTRY_HOST_NAME: localhost
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: SASL_SSL://kafka:9095
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: _schemas
      SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: SASL_SSL
      SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM: SCRAM-SHA-512
      SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG: >-
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="schema-registry" password="${SR_PASSWORD}";
      SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION: /etc/schema-registry/secrets/ca.crt
      SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_TYPE: PEM

networks:
  kafka-home-lab-net:
    external: true
EOF

# ---------- 5. 스토리지 포맷 (최초 1회) + 브로커 기동 ----------
say "5/7 스토리지 포맷 및 브로커 기동"
if docker volume inspect "$DATA_VOLUME" >/dev/null 2>&1 && [ -f .formatted ]; then
  echo "이미 포맷된 볼륨 재사용"
else
  docker volume create "$DATA_VOLUME" >/dev/null
  # admin 계정을 SCRAM 으로 부트스트랩하며 포맷 — 사내 구축과 같은 절차
  docker run --rm \
    -v "$DATA_VOLUME":/var/lib/kafka/data \
    -v "$SECRETS_DIR/format.properties":/tmp/format.properties:ro \
    "$KAFKA_IMAGE" /opt/kafka/bin/kafka-storage.sh format \
    -t "$KAFKA_CLUSTER_ID" -c /tmp/format.properties \
    --add-scram "SCRAM-SHA-512=[name=admin,password=$ADMIN_PASSWORD]"
  touch .formatted
fi
$KAFKA_DC up -d

echo "브로커 기동 대기 중..."
KCMD="$KAFKA_DC exec -T kafka /opt/kafka/bin"
ok=""
for _ in $(seq 1 30); do
  if $KCMD/kafka-topics.sh --bootstrap-server localhost:9094 \
       --command-config /etc/kafka/secrets/admin.properties --list >/dev/null 2>&1; then
    ok=1; break
  fi
  sleep 2
done
[ -n "$ok" ] || { $KAFKA_DC logs --tail=30 kafka; fail "브로커가 60초 내에 뜨지 않았습니다 (위 로그 확인)"; }
echo "브로커 정상 기동"

# ---------- 6. 계정·토픽·ACL (schema-registry, app) ----------
say "6/7 계정·토픽·ACL 생성"
ADMIN_OPTS="--bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin.properties"

$KCMD/kafka-configs.sh $ADMIN_OPTS --alter \
  --add-config "SCRAM-SHA-512=[password=$SR_PASSWORD]" \
  --entity-type users --entity-name schema-registry
$KCMD/kafka-configs.sh $ADMIN_OPTS --alter \
  --add-config "SCRAM-SHA-512=[password=$APP_PASSWORD]" \
  --entity-type users --entity-name app

$KCMD/kafka-topics.sh $ADMIN_OPTS --create --if-not-exists \
  --topic _schemas --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact
$KCMD/kafka-topics.sh $ADMIN_OPTS --create --if-not-exists \
  --topic sandbox.demo --partitions 3 --replication-factor 1

# Schema Registry: _schemas 읽기/쓰기 + 기동 시 compact 검증(DescribeConfigs) + 리더 선출 그룹
$KCMD/kafka-acls.sh $ADMIN_OPTS --add --allow-principal User:schema-registry \
  --operation Read --operation Write --operation Describe --operation DescribeConfigs \
  --topic _schemas
$KCMD/kafka-acls.sh $ADMIN_OPTS --add --allow-principal User:schema-registry \
  --operation Read --group schema-registry
# 실습 계정: sandbox.demo 토픽 + demo 컨슈머 그룹
$KCMD/kafka-acls.sh $ADMIN_OPTS --add --allow-principal User:app \
  --operation Read --operation Write --operation Describe --topic sandbox.demo
$KCMD/kafka-acls.sh $ADMIN_OPTS --add --allow-principal User:app \
  --operation Read --group demo

# ---------- 7. Schema Registry 기동 + 최종 검증 ----------
say "7/7 Schema Registry 기동 및 검증"
$SR_DC up -d
echo "Schema Registry 기동 대기 중..."
ok=""
for _ in $(seq 1 30); do
  if curl -sf http://localhost:8081/subjects >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
[ -n "$ok" ] || { $SR_DC logs --tail=30 schema-registry; fail "Schema Registry 가 60초 내에 뜨지 않았습니다 (위 로그 확인)"; }

echo
echo "토픽 목록:"
$KCMD/kafka-topics.sh $ADMIN_OPTS --list
echo "Schema Registry: $(curl -s http://localhost:8081/subjects)"

cat <<EOF

============================================================
 구축 완료!
============================================================
 작업 디렉터리 : $LAB_DIR
 자격증명     : $LAB_DIR/.env (커밋 금지) — 프로젝트별 .env 는 kafka/, schema-registry/ 에 분리
 Kafka        : localhost:9094 (SASL_SSL, 계정 admin / app)  ← compose 프로젝트 $KAFKA_PROJECT
 Schema Reg.  : http://localhost:8081                      ← compose 프로젝트 $SR_PROJECT

 [실습 — 메시지 보내기 (app 계정, 키:값 형식 입력)]
 cd $KAFKA_PROJECT
 docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \\
   --bootstrap-server localhost:9094 \\
   --producer.config /etc/kafka/secrets/app.properties \\
   --topic sandbox.demo --property parse.key=true --property key.separator=:

 [실습 — 메시지 받기 (다른 터미널)]
 docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \\
   --bootstrap-server localhost:9094 \\
   --consumer.config /etc/kafka/secrets/app.properties \\
   --topic sandbox.demo --group demo --from-beginning \\
   --property print.partition=true --property print.key=true

 [SR 만 재시작]  (cd $SR_PROJECT && docker compose restart)   # 브로커는 그대로
 [중지/재시작]   각 프로젝트 디렉터리에서 docker compose stop / start
 [완전 삭제]     (cd $SR_PROJECT && docker compose down) \\
              && (cd $KAFKA_PROJECT && docker compose down -v) && rm -rf $LAB_DIR
============================================================
EOF

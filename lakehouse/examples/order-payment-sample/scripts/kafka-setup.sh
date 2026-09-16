#!/usr/bin/env bash
#
# 1단계: kafka-home-lab 브로커에 order-service 계정, order.events / payment.events 토픽, ACL 을 만든다.
# 2단계: lakehouse-poc 의 connect 계정에 두 토픽 읽기 ACL 을 추가한다.
#
# 사용법: ./kafka-setup.sh            # 비밀번호는 ../backend/.env 에 기록 (없으면 생성)
# 요구:   kafka-home-lab 이 떠 있고 ~/kafka-home-lab/secrets/admin.properties 가 있을 것
#
set -euo pipefail

KAFKA_LAB_DIR="${KAFKA_LAB_DIR:-$HOME/kafka-home-lab}"
ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/backend/.env"
KCMD="docker compose --project-directory $KAFKA_LAB_DIR/kafka exec -T kafka /opt/kafka/bin"
A="--bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin.properties"

say() { printf '\n\033[1;34m==> %s\033[0m\n' "$1"; }

docker ps --format '{{.Names}}' | grep -q '^kafka-home-lab$' || { echo "kafka-home-lab 컨테이너가 실행 중이 아닙니다" >&2; exit 1; }

if [ -f "$ENV_FILE" ] && grep -q '^KAFKA_PASSWORD=' "$ENV_FILE"; then
  KAFKA_PASSWORD=$(grep '^KAFKA_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)
  echo "기존 비밀번호 재사용: $ENV_FILE"
else
  KAFKA_PASSWORD=$(openssl rand -hex 16)
  cat > "$ENV_FILE" <<EOF
# order-payment-sample 백엔드 환경변수 — 커밋 금지. `set -a; . ./.env; set +a` 로 읽는다
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_USER=order-service
KAFKA_PASSWORD=$KAFKA_PASSWORD
KAFKA_TRUSTSTORE_LOCATION=$KAFKA_LAB_DIR/secrets/ca.crt
TRINO_JDBC_URL=jdbc:trino://localhost:8080/lakehouse/commerce
TRINO_USER=poc
EOF
  chmod 600 "$ENV_FILE"
  echo "생성: $ENV_FILE"
fi

say "계정 order-service"
$KCMD/kafka-configs.sh $A --alter --add-config "SCRAM-SHA-512=[password=$KAFKA_PASSWORD]" \
  --entity-type users --entity-name order-service

say "토픽 order.events / payment.events (파티션 3, 키=order_id)"
for t in order.events payment.events; do
  $KCMD/kafka-topics.sh $A --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done

say "ACL: order-service 쓰기, connect 읽기"
for t in order.events payment.events; do
  $KCMD/kafka-acls.sh $A --add --allow-principal User:order-service \
    --operation Write --operation Describe --topic "$t" >/dev/null
  # 2단계 Iceberg Sink 가 읽는다 (lakehouse-poc 의 connect 계정)
  $KCMD/kafka-acls.sh $A --add --allow-principal User:connect \
    --operation Read --operation Describe --topic "$t" >/dev/null
done
# 멱등 프로듀서(enable.idempotence=true)는 클러스터 IdempotentWrite 가 필요하다
$KCMD/kafka-acls.sh $A --add --allow-principal User:order-service --operation IdempotentWrite --cluster >/dev/null
# Sink 커넥터의 컨슈머 그룹은 connect-<커넥터명>(+ -coord). 커넥터가 늘어나도 되게 connect- 접두사로 허용한다
$KCMD/kafka-acls.sh $A --add --allow-principal User:connect --operation Read --group connect- --resource-pattern-type prefixed >/dev/null
# 파이프라인 화면: 앱이 토픽 끝 오프셋과 커넥터 그룹의 커밋 오프셋(lag)을 읽는다
$KCMD/kafka-acls.sh $A --add --allow-principal User:order-service --operation Describe --group connect- --resource-pattern-type prefixed >/dev/null
$KCMD/kafka-acls.sh $A --list --topic order.events 2>/dev/null | grep -E 'principal' | sed 's/^/  /'

cat <<EOF

완료. 백엔드 실행:
  cd $(dirname "$ENV_FILE") && set -a && . ./.env && set +a && ./gradlew bootRun

토픽 확인 (다른 터미널):
  cd $KAFKA_LAB_DIR/kafka && docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \\
    --bootstrap-server localhost:9094 --consumer.config /etc/kafka/secrets/admin.properties \\
    --topic payment.events --from-beginning --property print.key=true
EOF

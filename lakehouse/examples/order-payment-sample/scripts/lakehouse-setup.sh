#!/usr/bin/env bash
#
# 2단계: lakehouse-poc(MinIO + REST Catalog + Trino + Kafka Connect) 에
#   - Iceberg 테이블 commerce.order_events / commerce.payment_events 를 DDL 로 만들고
#   - 토픽별 Iceberg Sink 커넥터 2개를 등록한다.
#
# 전제: ~/lakehouse-poc 가 `setup.sh --connect` 로 떠 있고, kafka-setup.sh 로 토픽·ACL 이 준비돼 있을 것.
# 사용법: ./lakehouse-setup.sh            # 테이블·커넥터 생성 (있으면 그대로)
#        ./lakehouse-setup.sh --reset    # 커넥터·컨슈머 그룹·테이블을 지우고 처음부터 (토픽의 이벤트는 그대로라 재적재된다)
#
set -euo pipefail

LAB_DIR="${LAKEHOUSE_LAB_DIR:-$HOME/lakehouse-poc}"
KAFKA_LAB_DIR="${KAFKA_LAB_DIR:-$HOME/kafka-home-lab}"
RESET=""; [ "${1:-}" = "--reset" ] && RESET=1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
TRINO="docker compose --project-directory $LAB_DIR exec -T trino trino --output-format=CSV_UNQUOTED"

say() { printf '\n\033[1;34m==> %s\033[0m\n' "$1"; }
fail() { printf '\033[1;31m오류: %s\033[0m\n' "$1" >&2; exit 1; }

[ -f "$LAB_DIR/.env" ] || fail "lakehouse-poc 를 찾을 수 없습니다: $LAB_DIR"
docker ps --format '{{.Names}}' | grep -q '^lakehouse-connect$' || fail "lakehouse-connect 가 실행 중이 아닙니다 (setup.sh --connect)"
# shellcheck disable=SC1091
set -a; . "$LAB_DIR/.env"; set +a

CONNECTORS="order-events-sink-app payment-events-sink-app"
if [ -n "$RESET" ]; then
  say "0. 초기화 — 커넥터 삭제 → 컨슈머 그룹 삭제 → 테이블 삭제"
  KCMD="docker compose --project-directory $KAFKA_LAB_DIR/kafka exec -T kafka /opt/kafka/bin"
  A="--bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin.properties"
  # 초기 PoC(setup.sh --connect)가 만든 order-events-sink 는 같은 테이블에 쓰므로 함께 지운다
  for c in $CONNECTORS order-events-sink; do
    curl -s -o /dev/null -X DELETE "$CONNECT_URL/connectors/$c" && echo "  커넥터 삭제: $c"
  done
  sleep 3
  for g in connect-order-events-sink-app connect-order-events-sink-app-coord connect-payment-events-sink-app connect-payment-events-sink-app-coord; do
    $KCMD/kafka-consumer-groups.sh $A --delete --group "$g" >/dev/null 2>&1 && echo "  그룹 삭제: $g" || true
  done
  for t in order_events payment_events; do
    $TRINO --execute "DROP TABLE IF EXISTS lakehouse.commerce.$t" && echo "  테이블 삭제: $t"
  done
fi

say "1. Iceberg 테이블 DDL (없으면 생성)"
# 이벤트 JSON 의 occurred_at 은 ISO-8601 문자열이라 varchar 로 받는다. 조회 SQL 에서 from_iso8601_timestamp 로 변환한다.
# 운영에서는 Connect SMT(TimestampConverter)로 timestamptz 로 바꾸고 day(occurred_at) 파티션을 건다.
$TRINO --execute "CREATE SCHEMA IF NOT EXISTS lakehouse.commerce"
$TRINO --execute "CREATE TABLE IF NOT EXISTS lakehouse.commerce.order_events (
  event_id varchar, event_type varchar, order_id varchar, customer_id varchar, item_name varchar,
  amount bigint, occurred_at varchar, schema_version bigint, source_system varchar
) WITH (format = 'PARQUET', format_version = 2)"
$TRINO --execute "CREATE TABLE IF NOT EXISTS lakehouse.commerce.payment_events (
  event_id varchar, event_type varchar, order_id varchar, payment_id varchar, customer_id varchar,
  amount bigint, method varchar, failure_reason varchar, occurred_at varchar, schema_version bigint, source_system varchar
) WITH (format = 'PARQUET', format_version = 2)"
$TRINO --execute "SHOW TABLES FROM lakehouse.commerce" | sed 's/^/  /'

say "2. 커넥터 등록 (order-events-sink-app, payment-events-sink-app)"
register() {
  local name=$1 topic=$2 table=$3
  sed -e "s|__NAME__|$name|g" -e "s|__TOPIC__|$topic|g" -e "s|__TABLE__|$table|g" \
      -e "s|__LAKEHOUSE_ACCESS_KEY__|$LAKEHOUSE_ACCESS_KEY|g" \
      -e "s|__LAKEHOUSE_SECRET_KEY__|$LAKEHOUSE_SECRET_KEY|g" \
      -e "s|__CONNECT_PASSWORD__|$CONNECT_PASSWORD|g" \
      "$SCRIPT_DIR/iceberg-sink.template.json" > "$LAB_DIR/connect/.$name.rendered.json"
  chmod 600 "$LAB_DIR/connect/.$name.rendered.json"
  curl -sf -X PUT -H 'Content-Type: application/json' --data @"$LAB_DIR/connect/.$name.rendered.json" \
    "$CONNECT_URL/connectors/$name/config" >/dev/null || fail "커넥터 $name 등록 실패"
}
register order-events-sink-app   order.events   commerce.order_events
register payment-events-sink-app payment.events commerce.payment_events

echo "태스크 상태 대기 중..."
for name in order-events-sink-app payment-events-sink-app; do
  ok=""
  for _ in $(seq 1 30); do
    st=$(curl -s "$CONNECT_URL/connectors/$name/status" || true)
    running=$(printf '%s' "$st" | grep -o '"state":"RUNNING"' | wc -l | tr -d ' ')
    if [ "$running" -ge 2 ] && ! printf '%s' "$st" | grep -q FAILED; then ok=1; break; fi
    sleep 2
  done
  [ -n "$ok" ] || { echo "$st" | head -c 600; echo; fail "$name 이 RUNNING 이 아닙니다"; }
  echo "  $name RUNNING"
done

cat <<EOF

완료. 커밋 주기(30초) 뒤 Trino 에서 확인:
  cd $LAB_DIR && docker compose exec -it trino trino
  SELECT event_type, count(*) FROM lakehouse.commerce.payment_events GROUP BY 1;
  SELECT event_type, count(*) FROM lakehouse.commerce.order_events GROUP BY 1;

백엔드 조회 API:  curl -s localhost:8090/api/analytics/payment-success-rate | jq
EOF

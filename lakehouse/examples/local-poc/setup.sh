#!/usr/bin/env bash
#
# Lakehouse 로컬 PoC 구축 스크립트
#
#   1단계 (기본)     : MinIO + Iceberg REST Catalog + Trino 를 띄우고, Trino 에서 테이블을
#                     만들어 넣고 읽는 것과 MinIO 에 파일이 생기는 것까지 확인한다.
#   2단계 (--connect): 기존 kafka-home-lab 브로커에 connect 계정·토픽·ACL 을 만들고
#                     Kafka Connect + Iceberg Sink 를 띄워, 샘플 주문 이벤트가 Iceberg 테이블에
#                     적재되어 Trino 에서 조회되는 것까지 확인한다.
#
# 이 예제 디렉터리의 compose·설정 파일을 <작업디렉터리>(기본 ~/lakehouse-poc) 로 복사하고
# 거기서 실행한다. 자격증명(.env)은 작업디렉터리에만 생긴다.
#
# 요구사항: docker (compose v2), curl, bash. 2단계는 kafka-home-lab 이 떠 있어야 한다.
# 사용법:   ./setup.sh [--connect] [작업디렉터리]
# 정리:     cd <작업디렉터리> && docker compose --profile connect down -v
#
set -euo pipefail

WITH_CONNECT=""
LAB_DIR="$HOME/lakehouse-poc"
for arg in "$@"; do
  case "$arg" in
    --connect) WITH_CONNECT=1 ;;
    -h|--help) sed -n 2,17p "$0"; exit 0 ;;
    *) LAB_DIR="$arg" ;;
  esac
done

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_LAB_DIR="${KAFKA_LAB_DIR:-$HOME/kafka-home-lab}"
ICEBERG_CONNECT_VERSION="1.9.2"
DC="docker compose --project-directory $LAB_DIR"
TRINO="$DC exec -T trino trino --output-format=CSV_UNQUOTED"

say()  { printf '\n\033[1;34m==> %s\033[0m\n' "$1"; }
fail() { printf '\033[1;31m오류: %s\033[0m\n' "$1" >&2; exit 1; }

# ---------- 0. 사전 점검 ----------
say "0. 사전 점검"
command -v docker >/dev/null || fail "docker 가 필요합니다"
command -v curl   >/dev/null || fail "curl 이 필요합니다"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 가 필요합니다"
docker info >/dev/null 2>&1 || fail "docker 데몬이 실행 중이 아닙니다"
for p in 8080 8181 9000 9001; do
  if ! docker ps --format '{{.Names}}' | grep -q '^lakehouse-' && lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "호스트 포트 $p 가 사용 중입니다 (docker-compose.yml 의 포트를 바꾸세요)"
  fi
done
if [ -n "$WITH_CONNECT" ]; then
  [ -f "$KAFKA_LAB_DIR/.env" ] || fail "kafka-home-lab 을 찾을 수 없습니다: $KAFKA_LAB_DIR (KAFKA_LAB_DIR 로 지정)"
  [ -f "$KAFKA_LAB_DIR/secrets/ca.crt" ] || fail "$KAFKA_LAB_DIR/secrets/ca.crt 가 없습니다"
  docker ps --format '{{.Names}}' | grep -q '^kafka-home-lab$' || fail "kafka-home-lab 컨테이너가 실행 중이 아닙니다"
  docker network inspect kafka-home-lab-net >/dev/null 2>&1 || fail "kafka-home-lab-net 네트워크가 없습니다"
fi

# ---------- 1. 작업 디렉터리 · 자격증명 ----------
say "1. 작업 디렉터리 준비: $LAB_DIR"
mkdir -p "$LAB_DIR/catalog-data"
chmod 777 "$LAB_DIR/catalog-data"   # iceberg-rest 컨테이너(비root 사용자)가 sqlite 파일을 쓴다
cp "$EXAMPLE_DIR/docker-compose.yml" "$LAB_DIR/"
rm -rf "$LAB_DIR/trino" "$LAB_DIR/minio" "$LAB_DIR/connect"
cp -R "$EXAMPLE_DIR/trino" "$EXAMPLE_DIR/minio" "$EXAMPLE_DIR/connect" "$LAB_DIR/"

if [ -f "$LAB_DIR/.env" ]; then
  echo "기존 .env 재사용"
else
  cat > "$LAB_DIR/.env" <<EOF
# lakehouse-poc 자격증명 — 커밋 금지
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=$(openssl rand -hex 12)
LAKEHOUSE_ACCESS_KEY=lakehouse
LAKEHOUSE_SECRET_KEY=$(openssl rand -hex 16)
CONNECT_PASSWORD=$(openssl rand -hex 16)
KAFKA_LAB_DIR=$KAFKA_LAB_DIR
ICEBERG_CONNECT_VERSION=$ICEBERG_CONNECT_VERSION
EOF
  chmod 600 "$LAB_DIR/.env"
fi
# shellcheck disable=SC1091
set -a; . "$LAB_DIR/.env"; set +a

# ---------- 2. 1단계 기동: MinIO + Catalog + Trino ----------
say "2. MinIO · Iceberg REST Catalog · Trino 기동"
$DC up -d minio minio-init iceberg-rest trino

echo "Trino 기동 대기 중 (최대 3분)..."
ok=""
for _ in $(seq 1 90); do
  if curl -sf http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false'; then ok=1; break; fi
  sleep 2
done
[ -n "$ok" ] || { $DC logs --tail=30 trino; fail "Trino 가 3분 내에 뜨지 않았습니다 (위 로그 확인)"; }
echo "Trino 정상 기동"

# ---------- 3. 1단계 검증: 테이블 쓰기·읽기·파일 확인 ----------
say "3. 1단계 검증 — Trino 로 테이블 생성·적재·조회"
$TRINO --execute "CREATE SCHEMA IF NOT EXISTS lakehouse.commerce"
$TRINO --execute "CREATE TABLE IF NOT EXISTS lakehouse.commerce.poc_smoke (id bigint, note varchar, created_at timestamp(6))"
$TRINO --execute "INSERT INTO lakehouse.commerce.poc_smoke VALUES (1, 'first commit', current_timestamp), (2, 'same batch', current_timestamp)"
$TRINO --execute "INSERT INTO lakehouse.commerce.poc_smoke VALUES (3, 'second commit', current_timestamp)"
rows=$($TRINO --execute "SELECT count(*) FROM lakehouse.commerce.poc_smoke")
echo "poc_smoke 행 수: $rows"
[ "$rows" -ge 3 ] || fail "행 수가 3 미만입니다"
echo "스냅샷 (커밋마다 1개):"
$TRINO --execute "SELECT snapshot_id, committed_at, operation FROM lakehouse.commerce.\"poc_smoke\$snapshots\" ORDER BY committed_at"
echo "MinIO 에 생긴 파일:"
docker run --rm --network lakehouse-poc-net \
  -e MC_HOST_local="http://$LAKEHOUSE_ACCESS_KEY:$LAKEHOUSE_SECRET_KEY@minio:9000" \
  quay.io/minio/mc:latest ls -r local/lakehouse/warehouse/ | sed 's/^/  /'

if [ -z "$WITH_CONNECT" ]; then
  cat <<EOF

============================================================
 1단계 완료 — MinIO + Iceberg REST Catalog + Trino
============================================================
 작업 디렉터리 : $LAB_DIR   (.env 커밋 금지)
 Trino        : http://localhost:8080   (사용자명 아무거나, 비밀번호 없음)
 MinIO 콘솔    : http://localhost:9001   (계정 .env 의 MINIO_ROOT_USER / PASSWORD)
 Catalog      : http://localhost:8181/v1/config

 [Trino CLI]   cd $LAB_DIR && docker compose exec -it trino trino
   SELECT * FROM lakehouse.commerce.poc_smoke;
   SELECT * FROM lakehouse.commerce."poc_smoke\$snapshots";
   SELECT file_path, record_count FROM lakehouse.commerce."poc_smoke\$files";

 [2단계]       $0 --connect      # kafka-home-lab 브로커에 Kafka Connect Iceberg Sink 연결
 [중지/재개]    cd $LAB_DIR && docker compose stop / start
 [완전 삭제]    cd $LAB_DIR && docker compose --profile connect down -v && rm -rf $LAB_DIR
============================================================
EOF
  exit 0
fi

# ---------- 4. 2단계: kafka-home-lab 에 connect 계정 · 토픽 · ACL ----------
say "4. kafka-home-lab 에 connect 계정·토픽·ACL 생성"
KDC="docker compose --project-directory $KAFKA_LAB_DIR/kafka"
KCMD="$KDC exec -T kafka /opt/kafka/bin"
ADMIN_OPTS="--bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin.properties"

$KCMD/kafka-configs.sh $ADMIN_OPTS --alter \
  --add-config "SCRAM-SHA-512=[password=$CONNECT_PASSWORD]" \
  --entity-type users --entity-name connect

# 홈랩은 auto.create.topics 가 꺼져 있으므로 Connect 내부 토픽과 데이터·제어 토픽을 미리 만든다
for t in lakehouse-connect-configs lakehouse-connect-offsets lakehouse-connect-status; do
  $KCMD/kafka-topics.sh $ADMIN_OPTS --create --if-not-exists --topic "$t" \
    --partitions 1 --replication-factor 1 --config cleanup.policy=compact
done
$KCMD/kafka-topics.sh $ADMIN_OPTS --create --if-not-exists --topic lakehouse.order.events \
  --partitions 3 --replication-factor 1
$KCMD/kafka-topics.sh $ADMIN_OPTS --create --if-not-exists --topic lakehouse-control-iceberg \
  --partitions 1 --replication-factor 1

ACL="$KCMD/kafka-acls.sh $ADMIN_OPTS --add --allow-principal User:connect"
# Connect 워커: 내부 토픽 3개 + 워커 그룹
for t in lakehouse-connect-configs lakehouse-connect-offsets lakehouse-connect-status; do
  $ACL --operation Read --operation Write --operation Describe --operation DescribeConfigs --topic "$t"
done
$ACL --operation Read --group lakehouse-connect
# 커넥터: 데이터 토픽 읽기, 제어 토픽 읽기·쓰기, 커넥터·코디네이터 컨슈머 그룹(접두사)
$ACL --operation Read --operation Describe --topic lakehouse.order.events
$ACL --operation Read --operation Write --operation Describe --topic lakehouse-control-iceberg
# 실측(1.9.2): 데이터 컨슈머 그룹 connect-order-events-sink, 코디네이터 그룹 connect-order-events-sink-coord,
# 워커별 제어 토픽 그룹 cg-control-<임의 UUID> (iceberg.control.group-id-prefix 기본값 cg-control).
# UUID 는 매번 바뀌므로 접두사 ACL 이 필요하다.
$ACL --operation Read --group connect-order-events-sink --resource-pattern-type prefixed
$ACL --operation Read --group cg-control- --resource-pattern-type prefixed
# Iceberg Sink 는 제어 토픽에 트랜잭션 프로듀서(exactly-once)를 쓴다
$ACL --operation Write --operation Describe --transactional-id '*'
$ACL --operation IdempotentWrite --cluster

# ---------- 5. Kafka Connect 기동 · 커넥터 등록 ----------
say "5. Kafka Connect 기동 (최초 1회 Iceberg 커넥터 설치 포함, 수 분 소요)"
$DC --profile connect up -d connect
# compose v2.19 에서 external 네트워크(kafka-home-lab-net) 연결이 누락된 채 컨테이너가 생성되는 경우가
# 있다(실측: 재구축 시). 그러면 kafka:9095 를 못 찾아 kafka-ready 에서 계속 재시작하므로 직접 붙인다.
if ! docker inspect lakehouse-connect --format '{{json .NetworkSettings.Networks}}' | grep -q kafka-home-lab-net; then
  echo "kafka-home-lab-net 연결 누락 → 직접 연결 후 재시작"
  docker network connect kafka-home-lab-net lakehouse-connect
  docker restart lakehouse-connect >/dev/null
fi
echo "Connect REST 대기 중 (최대 5분)..."
ok=""
for _ in $(seq 1 150); do
  if curl -sf http://localhost:8083/connector-plugins 2>/dev/null | grep -q IcebergSinkConnector; then ok=1; break; fi
  sleep 2
done
[ -n "$ok" ] || { $DC logs --tail=40 connect; fail "Connect 가 뜨지 않았거나 Iceberg 플러그인이 없습니다 (위 로그 확인)"; }

sed -e "s|__LAKEHOUSE_ACCESS_KEY__|$LAKEHOUSE_ACCESS_KEY|g" \
    -e "s|__LAKEHOUSE_SECRET_KEY__|$LAKEHOUSE_SECRET_KEY|g" \
    -e "s|__CONNECT_PASSWORD__|$CONNECT_PASSWORD|g" \
    "$LAB_DIR/connect/order-events-sink.json" > "$LAB_DIR/connect/.order-events-sink.rendered.json"
chmod 600 "$LAB_DIR/connect/.order-events-sink.rendered.json"
curl -sf -X PUT -H 'Content-Type: application/json' \
  --data @"$LAB_DIR/connect/.order-events-sink.rendered.json" \
  http://localhost:8083/connectors/order-events-sink/config >/dev/null \
  || fail "커넥터 등록 실패"
echo "커넥터 상태 대기 중..."
ok=""
for _ in $(seq 1 30); do
  st=$(curl -s http://localhost:8083/connectors/order-events-sink/status || true)
  # 커넥터 1 + 태스크 1 = RUNNING 두 번, FAILED 없음
  running=$(printf '%s' "$st" | grep -o '"state":"RUNNING"' | wc -l | tr -d ' ')
  if [ "$running" -ge 2 ] && ! printf '%s' "$st" | grep -q '"state":"FAILED"'; then ok=1; break; fi
  sleep 2
done
curl -s http://localhost:8083/connectors/order-events-sink/status; echo
[ -n "$ok" ] || { $DC logs --tail=60 connect | grep -iE 'error|exception' | tail -20 || true; fail "커넥터 또는 태스크가 RUNNING 이 아닙니다"; }

# ---------- 6. 샘플 이벤트 적재 · 조회 검증 ----------
say "6. 샘플 주문 이벤트 5건 발행 → 커밋 대기 → Trino 조회"
events=$(cat <<'EOF'
{"eventId":"evt-0001","eventType":"ORDER_CREATED","orderId":"order-2001","customerId":"customer-3001","amount":35000,"occurredAt":"2026-09-13T10:00:00+09:00"}
{"eventId":"evt-0002","eventType":"PAYMENT_COMPLETED","orderId":"order-2001","customerId":"customer-3001","amount":35000,"occurredAt":"2026-09-13T10:00:05+09:00"}
{"eventId":"evt-0003","eventType":"ORDER_CREATED","orderId":"order-2002","customerId":"customer-3002","amount":12000,"occurredAt":"2026-09-13T10:01:00+09:00"}
{"eventId":"evt-0004","eventType":"ORDER_CANCELLED","orderId":"order-2001","customerId":"customer-3001","amount":35000,"occurredAt":"2026-09-13T10:05:00+09:00"}
{"eventId":"evt-0005","eventType":"PAYMENT_COMPLETED","orderId":"order-2002","customerId":"customer-3002","amount":12000,"occurredAt":"2026-09-13T10:01:04+09:00"}
EOF
)
printf '%s\n' "$events" | $KCMD/kafka-console-producer.sh --bootstrap-server localhost:9094 \
  --producer.config /etc/kafka/secrets/admin.properties --topic lakehouse.order.events
echo "발행 완료. 커밋 주기(30초) 대기..."
ok=""
for _ in $(seq 1 45); do
  n=$($TRINO --execute "SELECT count(*) FROM lakehouse.commerce.order_events" 2>/dev/null || echo 0)
  if [ "${n:-0}" -ge 5 ]; then ok=1; break; fi
  sleep 4
done
[ -n "$ok" ] || { $DC logs --tail=60 connect | grep -iE 'error|exception|commit' | tail -20 || true; fail "3분 안에 order_events 에 5건이 보이지 않습니다"; }
echo "order_events 행 수: $n"
$TRINO --execute "SELECT eventid, eventtype, orderid, amount FROM lakehouse.commerce.order_events ORDER BY eventid"

cat <<EOF

============================================================
 2단계 완료 — Kafka → Kafka Connect Iceberg Sink → MinIO + Catalog → Trino
============================================================
 작업 디렉터리 : $LAB_DIR
 Connect      : http://localhost:8083/connectors/order-events-sink/status
 테이블        : lakehouse.commerce.order_events (30초마다 커밋)

 [이벤트 더 넣기 — admin 계정, JSON 한 줄 = 이벤트 1건]
 cd $KAFKA_LAB_DIR/kafka && docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \\
   --bootstrap-server localhost:9094 --producer.config /etc/kafka/secrets/admin.properties \\
   --topic lakehouse.order.events

 [장애 재현 — 누락·중복 확인]
 cd $LAB_DIR && docker compose kill connect      # 적재 중 강제 종료
 (이벤트 몇 건 더 발행)
 docker compose --profile connect up -d connect   # 재시작 → 마지막 커밋 다음부터 이어서 적재
 Trino: SELECT eventid, count(*) FROM lakehouse.commerce.order_events GROUP BY eventid HAVING count(*) > 1;

 [정리]  cd $LAB_DIR && docker compose --profile connect down -v && rm -rf $LAB_DIR
        (홈랩의 connect 계정·토픽·ACL 은 README 의 정리 절차 참고)
============================================================
EOF

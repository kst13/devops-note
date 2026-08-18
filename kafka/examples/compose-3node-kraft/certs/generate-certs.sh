#!/usr/bin/env bash
# 3노드 Kafka 클러스터용 TLS 인증서 일괄 생성 스크립트
#
# 사용법: 아래 "설정" 세 값만 고친 뒤 실행한다.
#   ./generate-certs.sh          # 생성 (기존 산출물이 있으면 중단)
#   ./generate-certs.sh --force  # 기존 산출물을 지우고 처음부터 재생성
#
# keytool 이 없는 환경(예: Java 미설치 PC)이면 apache/kafka 컨테이너 안에서
# 자동으로 자기 자신을 다시 실행하므로 Docker 만 있으면 된다.
# 산출물은 스크립트를 실행한 현재 디렉터리에 생긴다.
set -euo pipefail

# ===== 설정: 여기 세 값만 수정 =====
STOREPASS='__SET_ME__'    # keystore/truststore 비밀번호 (.env 의 비밀번호 3개와 동일하게)
VALID=3650                # 인증서 유효기간(일) — 3650 = 약 10년
NODES="kafka1:10.0.0.11 kafka2:10.0.0.12 kafka3:10.0.0.13"    # 이름:실제서버IP
# ===================================

# keytool 이 없으면 kafka 이미지 안에서 재실행 (현재 디렉터리를 /work 로 마운트)
if ! command -v keytool >/dev/null 2>&1; then
  echo "keytool 이 없어 apache/kafka 컨테이너 안에서 다시 실행합니다..."
  exec docker run --rm -v "$PWD":/work -w /work --entrypoint bash \
    apache/kafka:4.0.0 "./$(basename "$0")" "$@"
fi

# --- 설정 검증 ---
if [ "$STOREPASS" = '__SET_ME__' ] || [ "${#STOREPASS}" -lt 6 ]; then
  echo "오류: 스크립트 상단의 STOREPASS 를 6자 이상 실제 값으로 수정하세요." >&2
  exit 1
fi
for pair in $NODES; do
  ip="${pair##*:}"
  case "$ip" in
    *[!0-9.]* | "")
      echo "오류: NODES 의 IP 형식이 잘못되었습니다: ${pair}" >&2
      exit 1
      ;;
  esac
done

# --- 기존 산출물 처리 ---
if ls ./*.jks >/dev/null 2>&1; then
  if [ "${1:-}" = "--force" ]; then
    echo "==> --force: 기존 산출물을 지우고 다시 생성합니다"
    rm -f ./*.jks ./*.csr ./*.crt ./*.srl ca.key
  else
    echo "오류: 이미 생성된 파일(*.jks)이 있습니다. 재생성하려면 --force 로 실행하세요." >&2
    exit 1
  fi
fi

echo "==> 1/4 사설 CA 생성"
openssl req -new -x509 -keyout ca.key -out ca.crt -days "$VALID" -nodes \
  -subj "/CN=devops-note-kafka-CA" 2>/dev/null

echo "==> 2/4 공통 truststore 생성"
keytool -keystore truststore.jks -alias CARoot -import -file ca.crt \
  -storepass "$STOREPASS" -noprompt >/dev/null 2>&1

echo "==> 3/4 노드별 keystore 생성"
for pair in $NODES; do
  NODE="${pair%%:*}"
  IP="${pair##*:}"
  echo "    - ${NODE} (${IP})"
  keytool -keystore "${NODE}.keystore.jks" -alias "${NODE}" -validity "$VALID" \
    -genkey -keyalg RSA -storepass "$STOREPASS" -keypass "$STOREPASS" \
    -dname "CN=${NODE}" -ext "SAN=dns:${NODE},ip:${IP}" \
    -ext "EKU=serverAuth,clientAuth" 2>/dev/null
  keytool -keystore "${NODE}.keystore.jks" -alias "${NODE}" -certreq \
    -file "${NODE}.csr" -storepass "$STOREPASS" 2>/dev/null
  openssl x509 -req -CA ca.crt -CAkey ca.key -in "${NODE}.csr" \
    -out "${NODE}.crt" -days "$VALID" -CAcreateserial \
    -extfile <(printf "subjectAltName=DNS:%s,IP:%s\nextendedKeyUsage=serverAuth,clientAuth" "$NODE" "$IP") \
    2>/dev/null
  keytool -keystore "${NODE}.keystore.jks" -alias CARoot -import -file ca.crt \
    -storepass "$STOREPASS" -noprompt >/dev/null 2>&1
  keytool -keystore "${NODE}.keystore.jks" -alias "${NODE}" -import \
    -file "${NODE}.crt" -storepass "$STOREPASS" -noprompt >/dev/null 2>&1
done

echo "==> 4/4 SAN 검증"
FAIL=0
for pair in $NODES; do
  NODE="${pair%%:*}"
  IP="${pair##*:}"
  if keytool -list -v -keystore "${NODE}.keystore.jks" -storepass "$STOREPASS" 2>/dev/null \
    | grep -q "IPAddress: ${IP}"; then
    echo "    - ${NODE}: OK (IPAddress: ${IP})"
  else
    echo "    - ${NODE}: 실패 — 인증서 SAN 에 ${IP} 가 없습니다" >&2
    FAIL=1
  fi
done
[ "$FAIL" -eq 0 ] || exit 1

# 중간 산출물 정리 (keystore 에 이미 반입되어 더 이상 필요 없음)
# ca.crt 는 기동 후 TLS 검증(openssl s_client)과 앱 배포에 쓰이므로 남긴다.
for pair in $NODES; do
  rm -f "./${pair%%:*}.csr" "./${pair%%:*}.crt"
done
rm -f ./*.srl

echo
echo "완료. 생성된 파일:"
ls -l ./*.jks ca.key ca.crt 2>/dev/null
echo
echo "다음 단계 — 각 서버로 배포:"
for pair in $NODES; do
  NODE="${pair%%:*}"
  IP="${pair##*:}"
  echo "  scp ${NODE}.keystore.jks truststore.jks ow@${IP}:/home/ow/kafka/secret/"
done
echo "  * ca.key 는 서버에 올리지 말 것 — 재발급용으로 안전한 곳에 별도 보관"

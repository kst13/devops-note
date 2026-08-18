# TLS 인증서 생성 (사설 CA + 노드별 keystore/truststore)

[07-kraft-cluster-installation.md](../../../concepts/07-kraft-cluster-installation.md) 5장에서 참조하는 인증서를 만드는 절차입니다. 사설 CA 1개로 노드 3대의 keystore와 공통 truststore를 발급합니다.

> 생성된 `*.jks`, `*.key`, `*.crt` 와 비밀번호는 **저장소에 커밋하지 않습니다.** 이 폴더는 `.gitignore` 로 제외하고, 산출물은 배포 시 각 노드에 안전하게 전달합니다.

## 빠른 방법 — 스크립트로 일괄 생성

[generate-certs.sh](generate-certs.sh) 상단의 세 값(`STOREPASS`, `VALID`, `NODES`)만 고치고 실행하면(마지막에 출력되는 scp 안내를 맞추려면 `DEPLOY_USER`, `KAFKA_HOME_DIR` 도 함께) 아래 전 과정(CA → truststore → 노드별 keystore → SAN 검증)을 한 번에 수행합니다. keytool 이 없으면 apache/kafka 컨테이너 안에서 자동으로 재실행되므로 Docker 만 있으면 됩니다.

```bash
mkdir -p ~/kafka-certs && cd ~/kafka-certs
cp <저장소>/kafka/examples/compose-3node-kraft/certs/generate-certs.sh .
vi generate-certs.sh     # 상단 STOREPASS / NODES 수정
./generate-certs.sh      # 재생성은 ./generate-certs.sh --force
```

성공하면 노드별 SAN 검증 결과와 서버별 scp 명령까지 출력됩니다. 아래는 스크립트가 하는 일을 단계별로 설명한 것입니다 — 수동으로 하고 싶거나 원리를 알고 싶을 때 참고하세요.

## 사전 준비

```bash
# 노드 정보 (실제 호스트명/IP 로 바꾼다)
# kafka1=10.0.0.11, kafka2=10.0.0.12, kafka3=10.0.0.13
STOREPASS='__SET_ME__'      # keystore/truststore 비밀번호 (Secret Manager 로 관리)
VALID=3650                  # 유효기간(일)
```

## 1. 사설 CA 생성

```bash
openssl req -new -x509 -keyout ca.key -out ca.crt -days "$VALID" -nodes \
  -subj "/CN=devops-note-kafka-CA"
```

## 2. 공통 truststore 생성 (CA 신뢰)

모든 노드와 클라이언트가 같은 truststore 를 씁니다.

```bash
keytool -keystore truststore.jks -alias CARoot -import -file ca.crt \
  -storepass "$STOREPASS" -noprompt
```

## 3. 노드별 keystore 생성

노드마다 반복합니다. `SAN` 에 그 노드의 **호스트명과 IP 를 모두** 넣는 것이 핵심입니다 — compose 구성이 호스트 IP 로 광고·접속하므로 특히 `ip:` 항목이 없으면 hostname verification 에 실패합니다. 컨트롤러 리스너가 mTLS 이므로 serverAuth·clientAuth 를 함께 부여합니다.

```bash
# 예: kafka1 (다른 노드는 NODE/IP 만 바꿔 반복)
NODE=kafka1
IP=10.0.0.11

# 3-1. 키쌍 생성 (SAN 포함)
keytool -keystore "${NODE}.keystore.jks" -alias "${NODE}" -validity "$VALID" \
  -genkey -keyalg RSA -storepass "$STOREPASS" -keypass "$STOREPASS" \
  -dname "CN=${NODE}" \
  -ext "SAN=dns:${NODE},ip:${IP}" \
  -ext "EKU=serverAuth,clientAuth"

# 3-2. CSR 생성 → CA 서명
keytool -keystore "${NODE}.keystore.jks" -alias "${NODE}" -certreq -file "${NODE}.csr" \
  -storepass "$STOREPASS"

openssl x509 -req -CA ca.crt -CAkey ca.key -in "${NODE}.csr" -out "${NODE}.crt" \
  -days "$VALID" -CAcreateserial \
  -extfile <(printf "subjectAltName=DNS:%s,IP:%s\nextendedKeyUsage=serverAuth,clientAuth" "$NODE" "$IP")

# 3-3. CA 인증서와 서명된 노드 인증서를 keystore 에 반입
keytool -keystore "${NODE}.keystore.jks" -alias CARoot -import -file ca.crt \
  -storepass "$STOREPASS" -noprompt
keytool -keystore "${NODE}.keystore.jks" -alias "${NODE}" -import -file "${NODE}.crt" \
  -storepass "$STOREPASS" -noprompt
```

kafka2(10.0.0.12), kafka3(10.0.0.13) 에 대해 `NODE`/`IP` 만 바꿔 반복합니다.

## 4. 배치

각 노드의 `${KAFKA_HOME_DIR}/secret/` 디렉터리(`.env` 의 `KAFKA_HOME_DIR`, compose 에서 `/etc/kafka/secrets` 로 마운트)에 아래를 둡니다.

```text
kafka1 노드:  ${KAFKA_HOME_DIR}/secret/kafka1.keystore.jks  +  truststore.jks
kafka2 노드:  ${KAFKA_HOME_DIR}/secret/kafka2.keystore.jks  +  truststore.jks
kafka3 노드:  ${KAFKA_HOME_DIR}/secret/kafka3.keystore.jks  +  truststore.jks
```

컨테이너 실행 UID(1000)가 읽을 수 있어야 합니다:

```bash
sudo chown -R 1000:1000 "${KAFKA_HOME_DIR}/secret"
chmod 600 "${KAFKA_HOME_DIR}"/secret/*.jks
```

`.env` 의 `KAFKA_KEYSTORE_FILE` 을 각 노드의 keystore 파일명으로 맞춥니다.

## 5. 클라이언트용 truststore

애플리케이션(MSA 앱)과 CLI 도 같은 `truststore.jks` 로 CA 를 신뢰해야 합니다. 클라이언트 설정 예시는 [commands/kafka-operations-cheatsheet.md](../../../commands/kafka-operations-cheatsheet.md) 를 참고합니다.

## 확인

```bash
# 발급된 노드 인증서의 SAN 확인 (advertised 주소와 일치해야 함)
keytool -list -v -keystore kafka1.keystore.jks -storepass "$STOREPASS" | grep -A1 "SubjectAlternativeName"

# 리스너 TLS 동작 확인 (기동 후)
openssl s_client -connect 10.0.0.11:9094 -CAfile ca.crt </dev/null
```

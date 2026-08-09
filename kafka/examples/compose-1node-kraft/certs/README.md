# TLS 인증서 생성 (localhost 단일 keystore)

[3노드 절차](../../compose-3node-kraft/certs/README.md)의 축소판입니다. 사설 CA 1개로 keystore 1개와 truststore를 발급합니다. advertised 주소가 `localhost`이므로 SAN 도 `localhost`/`127.0.0.1`로 넣습니다 — 이래야 hostname verification(`ssl.endpoint.identification.algorithm=https`)을 켠 채로 동작합니다.

> 생성된 `*.jks`, `*.key`, `*.crt` 와 비밀번호는 **저장소에 커밋하지 않습니다.** 루트 `.gitignore` 가 이 폴더의 산출물을 제외합니다.

## 사전 준비

```bash
STOREPASS='__SET_ME__'      # keystore/truststore 비밀번호 (.env 값과 일치시킨다)
                            # 아래 3-1 에서 -keypass 에도 같은 값을 주므로 키 비밀번호까지
                            # 이 값이 된다. .env 의 KAFKA_KEYSTORE_PASSWORD,
                            # KAFKA_KEY_PASSWORD, KAFKA_TRUSTSTORE_PASSWORD 셋 다 같은 값으로 채운다.
VALID=3650                  # 유효기간(일)
```

## 1. 사설 CA 생성

```bash
openssl req -new -x509 -keyout ca.key -out ca.crt -days "$VALID" -nodes \
  -subj "/CN=devops-note-kafka-local-CA"
```

## 2. truststore 생성 (CA 신뢰)

브로커와 클라이언트(CLI 포함)가 같은 truststore 를 씁니다.

```bash
keytool -keystore truststore.jks -alias CARoot -import -file ca.crt \
  -storepass "$STOREPASS" -noprompt
```

## 3. keystore 생성 (SAN=localhost)

컨트롤러 리스너가 mTLS 이므로 serverAuth·clientAuth 를 함께 부여합니다.

```bash
# 3-1. 키쌍 생성 (SAN 포함)
keytool -keystore kafka.keystore.jks -alias kafka -validity "$VALID" \
  -genkey -keyalg RSA -storepass "$STOREPASS" -keypass "$STOREPASS" \
  -dname "CN=localhost" \
  -ext "SAN=dns:localhost,ip:127.0.0.1" \
  -ext "EKU=serverAuth,clientAuth"

# 3-2. CSR 생성 → CA 서명
keytool -keystore kafka.keystore.jks -alias kafka -certreq -file kafka.csr \
  -storepass "$STOREPASS"

openssl x509 -req -CA ca.crt -CAkey ca.key -in kafka.csr -out kafka.crt \
  -days "$VALID" -CAcreateserial \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth")

# 3-3. CA 인증서와 서명된 인증서를 keystore 에 반입
keytool -keystore kafka.keystore.jks -alias CARoot -import -file ca.crt \
  -storepass "$STOREPASS" -noprompt
keytool -keystore kafka.keystore.jks -alias kafka -import -file kafka.crt \
  -storepass "$STOREPASS" -noprompt
```

## 4. 배치

compose 가 마운트하는 `secrets/` 디렉터리(직접 생성)에 둡니다.

```text
secrets/kafka.keystore.jks
secrets/truststore.jks
```

## 확인

```bash
# SAN 에 localhost/127.0.0.1 이 들어갔는지 확인
keytool -exportcert -keystore kafka.keystore.jks -alias kafka -storepass "$STOREPASS" | \
  openssl x509 -text -noout | grep -A1 "Subject Alternative Name"

# 리스너 TLS 동작 확인 (기동 후)
openssl s_client -connect localhost:9094 -CAfile ca.crt </dev/null
```

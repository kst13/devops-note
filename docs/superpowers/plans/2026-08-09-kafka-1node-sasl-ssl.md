# 단일 노드 KRaft SASL_SSL 예제 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 로컬 학습용 단일 노드 Kafka(KRaft combined, SASL_SSL + mTLS) docker-compose 예제를 `kafka/examples/compose-1node-kraft/`에 추가한다.

**Architecture:** 기존 `compose-3node-kraft` 예제와 리스너 3개(INTERNAL/CONTROLLER/CLIENT) 구조를 동일하게 유지하고, 단일 노드 값(voters 1개, localhost, RF=1, named volume)만 바꾼다. 스펙: `docs/superpowers/specs/2026-08-09-kafka-1node-sasl-ssl-design.md`.

**Tech Stack:** Docker Compose, apache/kafka:4.0.0, keytool/openssl(인증서), Next.js 콘텐츠 파이프라인(web/scripts/sync-content.mjs).

## Global Constraints

- 모든 산문은 한국어. ATX 헤딩, 언어 태그 붙은 펜스 코드 블록, 상대 링크.
- 파일명은 lowercase kebab-case.
- 시크릿은 자리 표시자(`__SET_ME__`)만. 실제 비밀번호·운영 호스트명 커밋 금지.
- YAML 들여쓰기 2칸. 설정의 *이유*를 주석으로 설명.
- 이미지 태그는 `apache/kafka:4.0.0` 고정 (3노드 예제와 동일).
- 커밋 메시지: 짧은 명령형 영어 제목 (예: `Add single-node Kafka SASL_SSL example`).
- `web/app/data/content.generated.json`은 생성 파일 — 손으로 편집 금지, `npm run sync-content`로만 갱신.
- 루트 `.gitignore`가 이미 `kafka/examples/**`의 `.env`/`secrets/`/인증서 산출물을 제외하므로 gitignore 수정은 불필요.

---

### Task 1: docker-compose.yml + .env.example

**Files:**
- Create: `kafka/examples/compose-1node-kraft/docker-compose.yml`
- Create: `kafka/examples/compose-1node-kraft/.env.example`

**Interfaces:**
- Consumes: 없음 (첫 태스크)
- Produces: 서비스명 `kafka`, keystore 파일명 `kafka.keystore.jks`, truststore 파일명 `truststore.jks`, 시크릿 마운트 경로 `/etc/kafka/secrets/`, env 변수명 `KAFKA_KEYSTORE_PASSWORD`/`KAFKA_KEY_PASSWORD`/`KAFKA_TRUSTSTORE_PASSWORD`/`KAFKA_INTER_BROKER_USER`/`KAFKA_INTER_BROKER_PASSWORD` — Task 2·3의 문서가 이 이름들을 그대로 참조한다.

- [ ] **Step 1: docker-compose.yml 작성**

`kafka/examples/compose-1node-kraft/docker-compose.yml`:

```yaml
# 로컬 학습용 단일 노드 Docker Compose (KRaft combined, SASL_SSL + mTLS)
#
# compose-3node-kraft 와 "노드 수만 다른" 짝 예제다. 리스너 3개 구조를 그대로
# 유지해 두 파일을 나란히 놓고 비교할 수 있게 했다. 바뀐 것은 voters 1개,
# advertised=localhost, 복제 계수 1, named volume 뿐이다.
#
# 운영 구성이 아니다. 실서버 배포는 ../compose-3node-kraft 를 사용한다.
# 최초 1회 스토리지 포맷이 필요하다. README.md 실행 순서 참고.

services:
  kafka:
    image: apache/kafka:4.0.0
    container_name: kafka
    restart: unless-stopped
    ports:
      - "9092:9092"   # INTERNAL (단일 노드에선 실사용 없음, 아래 주석 참고)
      - "9093:9093"   # CONTROLLER (쿼럼)
      - "9094:9094"   # CLIENT (앱 접속)
    environment:
      # --- KRaft 역할/식별 ---
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093

      # --- 리스너 정의 ---
      # 단일 노드라 브로커 간 트래픽이 실제로는 발생하지 않지만,
      # inter.broker.listener.name 은 반드시 존재하는 리스너를 가리켜야 하므로
      # 3노드 예제와의 대칭성을 위해 전용 INTERNAL 리스너를 유지한다.
      KAFKA_LISTENERS: INTERNAL://:9092,CONTROLLER://:9093,CLIENT://:9094
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://localhost:9092,CLIENT://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:SASL_SSL,CONTROLLER:SSL,CLIENT:SASL_SSL
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # --- SASL/SCRAM (INTERNAL, CLIENT) ---
      KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
      KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-512
      # 브로커 간 인증에 쓸 계정(스토리지 포맷 시 --add-scram 으로 부트스트랩한 admin)
      KAFKA_LISTENER_NAME_INTERNAL_SASL_JAAS_CONFIG: >-
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="${KAFKA_INTER_BROKER_USER}" password="${KAFKA_INTER_BROKER_PASSWORD}";

      # --- TLS (모든 SSL/SASL_SSL 리스너 공통) ---
      KAFKA_SSL_KEYSTORE_LOCATION: /etc/kafka/secrets/kafka.keystore.jks
      KAFKA_SSL_KEYSTORE_PASSWORD: ${KAFKA_KEYSTORE_PASSWORD}
      KAFKA_SSL_KEY_PASSWORD: ${KAFKA_KEY_PASSWORD}
      KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/truststore.jks
      KAFKA_SSL_TRUSTSTORE_PASSWORD: ${KAFKA_TRUSTSTORE_PASSWORD}
      # 컨트롤러 리스너는 mTLS: 클라이언트 인증서를 필수로 요구 (3노드와 동일)
      KAFKA_SSL_CLIENT_AUTH: required
      # 인증서 SAN 이 localhost/127.0.0.1 이므로 hostname verification 을 켠 채 동작
      KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: https

      # --- 단일 노드: 복제 계수 전부 1 (브로커가 1대라 2 이상은 기동 불가) ---
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

      # --- 저장 위치 ---
      KAFKA_LOG_DIRS: /var/lib/kafka/data

    volumes:
      # 로컬 학습용이므로 호스트 경로 대신 named volume 을 쓴다
      - kafka-data:/var/lib/kafka/data
      - ./secrets:/etc/kafka/secrets:ro

volumes:
  kafka-data:
```

- [ ] **Step 2: .env.example 작성**

`kafka/examples/compose-1node-kraft/.env.example`:

```bash
# .env 로 복사한 뒤 비밀번호를 채운다.
#   cp .env.example .env
# .env 는 커밋 금지 — 루트 .gitignore 가 kafka/examples/**/.env 를 제외한다.
#
# 단일 노드라 노드 식별 값(NODE_ID, advertised 주소, keystore 파일명)은
# docker-compose.yml 에 고정했고, 여기엔 시크릿만 남는다.

KAFKA_KEYSTORE_PASSWORD=__SET_ME__
KAFKA_KEY_PASSWORD=__SET_ME__
KAFKA_TRUSTSTORE_PASSWORD=__SET_ME__

# 브로커 간 인증 계정 (스토리지 포맷 시 --add-scram 으로 만든 admin 계정과 일치해야 함)
KAFKA_INTER_BROKER_USER=admin
KAFKA_INTER_BROKER_PASSWORD=__SET_ME__
```

- [ ] **Step 3: compose 문법 검증**

Run:

```bash
cd kafka/examples/compose-1node-kraft
docker compose --env-file .env.example config >/dev/null && echo OK
```

Expected: `OK` 출력, 경고 없음. (`--env-file .env.example`을 쓰므로 `.env`를 만들 필요 없음. docker CLI가 없는 환경이면 이 단계를 건너뛰고 커밋 메시지에 검증 생략을 남기지 말 것 — 대신 사용자에게 보고.)

- [ ] **Step 4: Commit**

```bash
git add kafka/examples/compose-1node-kraft/docker-compose.yml kafka/examples/compose-1node-kraft/.env.example
git commit -m "Add single-node Kafka SASL_SSL compose and env template"
```

---

### Task 2: certs/README.md (localhost SAN 인증서 절차)

**Files:**
- Create: `kafka/examples/compose-1node-kraft/certs/README.md`

**Interfaces:**
- Consumes: Task 1의 파일명 규약 — `kafka.keystore.jks`, `truststore.jks`, 배치 위치 `secrets/`.
- Produces: 인증서 생성 절차 문서. Task 3의 README가 이 문서를 1단계로 링크한다.

- [ ] **Step 1: certs/README.md 작성**

`kafka/examples/compose-1node-kraft/certs/README.md`:

````markdown
# TLS 인증서 생성 (localhost 단일 keystore)

[3노드 절차](../../compose-3node-kraft/certs/README.md)의 축소판입니다. 사설 CA 1개로 keystore 1개와 truststore를 발급합니다. advertised 주소가 `localhost`이므로 SAN 도 `localhost`/`127.0.0.1`로 넣습니다 — 이래야 hostname verification(`ssl.endpoint.identification.algorithm=https`)을 켠 채로 동작합니다.

> 생성된 `*.jks`, `*.key`, `*.crt` 와 비밀번호는 **저장소에 커밋하지 않습니다.** 루트 `.gitignore` 가 이 폴더의 산출물을 제외합니다.

## 사전 준비

```bash
STOREPASS='__SET_ME__'      # keystore/truststore 비밀번호 (.env 값과 일치시킨다)
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
keytool -list -v -keystore kafka.keystore.jks -storepass "$STOREPASS" | grep -A1 "SubjectAlternativeName"

# 리스너 TLS 동작 확인 (기동 후)
openssl s_client -connect localhost:9094 -CAfile ca.crt </dev/null
```
````

- [ ] **Step 2: 절차 스모크 테스트 (keytool·openssl 있을 때)**

문서의 명령을 스크래치 디렉터리에서 그대로 실행해 절차가 실제로 완주되는지 확인한다. `STOREPASS='smoketest123'`으로 치환해 1→2→3 순서로 실행 후:

```bash
keytool -list -v -keystore kafka.keystore.jks -storepass smoketest123 | grep -A1 "SubjectAlternativeName"
```

Expected: `DNSName: localhost` 와 `IPAddress: 127.0.0.1` 포함. 끝나면 스크래치 산출물 삭제. keytool 이 없는 환경이면 이 단계를 건너뛰고 사용자에게 보고한다.

- [ ] **Step 3: Commit**

```bash
git add kafka/examples/compose-1node-kraft/certs/README.md
git commit -m "Add localhost cert guide for single-node Kafka example"
```

---

### Task 3: README.md + 3노드 README 상호 링크

**Files:**
- Create: `kafka/examples/compose-1node-kraft/README.md`
- Modify: `kafka/examples/compose-3node-kraft/README.md:5` (인용 블록 아래에 한 줄 추가)

**Interfaces:**
- Consumes: Task 1의 env 변수명·파일명, Task 2의 인증서 절차 링크.
- Produces: 예제 진입점 README. Task 4의 sync-content 가 이 파일을 사이트 문서로 수집한다 (첫 `# H1`이 제목, `## H2`가 목차).

- [ ] **Step 1: README.md 작성**

`kafka/examples/compose-1node-kraft/README.md`:

````markdown
# Docker Compose 단일 노드 KRaft 예제 (SASL_SSL)

로컬 PC 한 대에서 SASL_SSL(TLS 암호화 + SASL/SCRAM 인증) 흐름을 학습·검증하기 위한 단일 노드 구성입니다. [3노드 예제](../compose-3node-kraft/README.md)와 **노드 수만 다르고** 리스너 구조·보안 설정은 동일합니다 — 클러스터로 확장하려면 3노드 예제로 넘어갑니다.

> 운영 구성이 아닙니다. advertised 주소가 `localhost` 고정이고 복제 계수가 1이라 장애 허용이 없습니다.

- 보안 개념: [보안: 암호화(TLS)와 인증(SASL·mTLS)](../../concepts/10-security-tls-and-auth.md)
- 클러스터 설치: [KRaft 3노드 클러스터 설치 및 설정 방법](../../concepts/07-kraft-cluster-installation.md)

## 구성 파일

```text
docker-compose.yml   단일 노드 값 고정 (시크릿만 .env 로 주입)
.env.example         .env 로 복사해 비밀번호 지정
certs/README.md      localhost SAN 인증서 생성 절차
secrets/             (직접 생성) keystore/truststore 배치 위치, .gitignore 대상
```

## 실행 순서

1. **인증서 생성** — [certs/README.md](certs/README.md) 절차로 CA·keystore·truststore 를 만들고 `secrets/` 에 배치.

2. **.env 작성**

   ```bash
   cp .env.example .env
   # 비밀번호 4개를 채운다. KAFKA_INTER_BROKER_PASSWORD 는 4단계 --add-scram 값과 일치해야 한다.
   ```

3. **클라이언트 설정 파일 작성** — CLI 접속에 쓸 `secrets/client-sasl-ssl.properties`:

   ```properties
   security.protocol=SASL_SSL
   sasl.mechanism=SCRAM-SHA-512
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
     username="admin" password="__SET_ME__";
   ssl.truststore.location=/etc/kafka/secrets/truststore.jks
   ssl.truststore.password=__SET_ME__
   ssl.endpoint.identification.algorithm=https
   ```

4. **클러스터 ID 생성 + 스토리지 포맷 (최초 1회)** — SCRAM admin 계정을 함께 부트스트랩합니다.

   ```bash
   KAFKA_CLUSTER_ID=$(docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid)

   docker compose run --rm kafka \
     /opt/kafka/bin/kafka-storage.sh format \
     --cluster-id "$KAFKA_CLUSTER_ID" \
     --config /etc/kafka/server.properties \
     --add-scram 'SCRAM-SHA-512=[name=admin,password=CHANGE_ME_ADMIN]'
   ```

   `CHANGE_ME_ADMIN` 은 `.env` 의 `KAFKA_INTER_BROKER_PASSWORD`, 3단계 client 설정의 `password` 와 같은 값이어야 합니다.

5. **기동**

   ```bash
   docker compose up -d
   docker compose logs -f kafka   # "Kafka Server started" 확인
   ```

6. **접속 검증** — TLS 와 SASL 인증이 실제로 도는지 확인합니다.

   ```bash
   # TLS 핸드셰이크 확인 (Verify return code: 0 이면 정상)
   openssl s_client -connect localhost:9094 -CAfile certs/ca.crt </dev/null

   # SASL_SSL 로 broker API 확인
   docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
     --bootstrap-server localhost:9094 \
     --command-config /etc/kafka/secrets/client-sasl-ssl.properties
   ```

7. **토픽 생성과 스모크 테스트** — 단일 노드이므로 RF1 로 만듭니다.

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9094 \
     --command-config /etc/kafka/secrets/client-sasl-ssl.properties \
     --create --topic hello-secure --partitions 3 --replication-factor 1

   # 프로듀서 (보낸 뒤 Ctrl+C)
   docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9094 \
     --producer.config /etc/kafka/secrets/client-sasl-ssl.properties \
     --topic hello-secure

   # 컨슈머 (받은 뒤 Ctrl+C)
   docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9094 \
     --consumer.config /etc/kafka/secrets/client-sasl-ssl.properties \
     --topic hello-secure --from-beginning
   ```

8. **별도 SCRAM 계정 생성 (선택)** — 앱을 붙일 때는 admin 을 그대로 쓰지 말고 서비스 계정을 만듭니다.

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
     --bootstrap-server localhost:9094 \
     --command-config /etc/kafka/secrets/client-sasl-ssl.properties \
     --alter --add-config 'SCRAM-SHA-512=[password=CHANGE_ME_APP]' \
     --entity-type users --entity-name my-app
   ```

## 3노드와 뭐가 다른가

| 항목 | 이 예제 (1노드) | 3노드 |
| --- | --- | --- |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@localhost:9093` | `1@kafka1:9093,2@kafka2:9093,3@kafka3:9093` |
| advertised 주소 | `localhost` 고정 | 실 IP (.env 주입) |
| 복제 계수 (RF/min.insync 등) | 전부 1 | 3 / 2 |
| 데이터 볼륨 | named volume | 호스트 경로 `/data/kafka` |
| INTERNAL 리스너 | 구조상 유지 (실사용 없음) | 브로커 간 복제 트래픽 |

리스너 3개 구조, SCRAM-SHA-512, keystore/truststore, 컨트롤러 mTLS, hostname verification 은 동일합니다.

## 주의

- `.env` 와 `secrets/`, `certs/` 산출물은 **커밋하지 않습니다** (루트 `.gitignore` 가 제외).
- 앱을 붙일 때는 admin 대신 별도 SCRAM 계정을 만드세요 — [운영 치트시트](../../commands/kafka-operations-cheatsheet.md)와 07 문서 9장 참고.
- 데이터를 초기화하려면 `docker compose down -v` 로 named volume 까지 지운 뒤 4단계(포맷)부터 다시 합니다.
````

- [ ] **Step 2: 3노드 README에 상호 링크 추가**

`kafka/examples/compose-3node-kraft/README.md` 5행의 인용 블록(PLAINTEXT 안내) 바로 아래에 한 줄 추가:

```markdown
> 한 대로 SASL_SSL 흐름만 먼저 익히려면 단일 노드 버전을 쓰세요 → [../compose-1node-kraft](../compose-1node-kraft/README.md). 노드 수만 다르고 보안 구성은 같습니다.
```

- [ ] **Step 3: 링크 검증**

Run:

```bash
ls kafka/examples/compose-1node-kraft/certs/README.md \
   kafka/examples/compose-3node-kraft/README.md \
   kafka/concepts/10-security-tls-and-auth.md \
   kafka/concepts/07-kraft-cluster-installation.md \
   kafka/commands/kafka-operations-cheatsheet.md
```

Expected: 5개 경로 모두 존재 (README 의 상대 링크 대상 확인).

- [ ] **Step 4: Commit**

```bash
git add kafka/examples/compose-1node-kraft/README.md kafka/examples/compose-3node-kraft/README.md
git commit -m "Add single-node Kafka SASL_SSL example README with cross-links"
```

---

### Task 4: 웹 콘텐츠 반영 + 검증

**Files:**
- Modify: `web/app/data/content.generated.json` (`npm run sync-content` 으로만 재생성)

**Interfaces:**
- Consumes: Task 2·3 이 만든 Markdown 파일들.
- Produces: 사이트에 새 문서 2건(examples 카테고리) 노출.

- [ ] **Step 1: 콘텐츠 재생성**

```bash
cd web && npm run sync-content
```

Expected: 종료 코드 0. `git diff --stat web/app/data/content.generated.json` 에 변경이 잡히고, diff 안에 `compose-1node-kraft` 가 등장.

- [ ] **Step 2: lint + test**

```bash
cd web && npm run lint && npm test
```

Expected: lint 통과, 테스트 2건 PASS. (테스트는 빌드 후 렌더된 HTML 의 리터럴 문자열을 검증하므로, 실패 시 새 문서가 기존 단언을 깨뜨렸는지 확인.)

- [ ] **Step 3: 공백 오류 검사**

```bash
git diff --check
```

Expected: 출력 없음.

- [ ] **Step 4: Commit**

```bash
git add web/app/data/content.generated.json
git commit -m "Sync web content for single-node Kafka example"
```

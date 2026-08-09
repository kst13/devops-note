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

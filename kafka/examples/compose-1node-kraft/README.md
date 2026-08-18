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
   # 비밀번호 4개를 채운다. KAFKA_CLUSTER_ID 는 4단계에서 채운다.
   ```

   `KAFKA_INTER_BROKER_PASSWORD` 는 4단계에서 `--add-scram` 으로 심을 admin 비밀번호가 됩니다 — 아래 명령이 `.env` 를 그대로 읽어 쓰므로 따로 맞출 값은 없습니다.

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

   `username`/`password` 는 `.env` 의 `KAFKA_INTER_BROKER_USER`/`KAFKA_INTER_BROKER_PASSWORD`, `ssl.truststore.password` 는 `KAFKA_TRUSTSTORE_PASSWORD` 와 같은 값입니다. 클라이언트는 truststore 만 있으면 됩니다 — SASL_SSL 리스너에서는 브로커가 클라이언트 인증서를 요구하지 않고(`ssl.client.auth` 는 SSL 인 CONTROLLER 리스너에만 적용됩니다) SCRAM 으로 신원을 확인합니다.

4. **클러스터 ID 생성 + 스토리지 포맷 (최초 1회)** — SCRAM admin 계정을 함께 부트스트랩합니다.

   ```bash
   # 4-1. 클러스터 ID 를 만들어 .env 에 기록한다 (compose 가 CLUSTER_ID 로 넘긴다)
   KAFKA_CLUSTER_ID=$(docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid)
   echo "KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID" >> .env

   # 4-2. .env 를 셸로 읽어 포맷 컨테이너에 넘긴다
   set -a; . ./.env; set +a

   docker compose run --rm --entrypoint bash \
     -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
     -e ADMIN_USER="$KAFKA_INTER_BROKER_USER" \
     -e ADMIN_PASSWORD="$KAFKA_INTER_BROKER_PASSWORD" \
     kafka -c '
       set -e
       # (a) 이미지 진입점과 같은 렌더러로 env 를 /opt/kafka/config/server.properties 로 옮긴다.
       #     렌더러는 곧바로 내장 포맷도 실행하는데 --add-scram 을 지원하지 않으므로,
       #     log.dirs 를 임시 경로로 돌려 그 결과를 버린다 (데이터 볼륨은 건드리지 않는다).
       mkdir -p /tmp/discard
       KAFKA_LOG_DIRS=/tmp/discard /opt/kafka/bin/kafka-run-class.sh kafka.docker.KafkaDockerWrapper setup \
         --default-configs-dir /etc/kafka/docker \
         --mounted-configs-dir /mnt/shared/config \
         --final-configs-dir /opt/kafka/config

       # (b) 실제 데이터 디렉터리를 SCRAM admin 과 함께 포맷한다
       sed -i "s|^log.dirs=.*|log.dirs=/var/lib/kafka/data|" /opt/kafka/config/server.properties
       /opt/kafka/bin/kafka-storage.sh format \
         --cluster-id "$CLUSTER_ID" \
         --config /opt/kafka/config/server.properties \
         --add-scram "SCRAM-SHA-512=[name=$ADMIN_USER,password=$ADMIN_PASSWORD]"
     '
   ```

   `Formatting metadata directory /var/lib/kafka/data` 가 찍히면 성공입니다.

   왜 이렇게 도는지: `docker compose run` 에 명령을 직접 주면 이미지 진입점(`/etc/kafka/docker/run`)이 건너뛰어져 `KAFKA_*` 환경변수가 properties 로 렌더링되지 않습니다. 그래서 렌더러(`KafkaDockerWrapper setup`)를 직접 호출해 env 가 반영된 `server.properties` 를 만든 뒤 그것으로 포맷합니다. `/etc/kafka/server.properties` 라는 경로는 이 이미지에 존재하지 않습니다 (기본값은 `/etc/kafka/docker/server.properties`, 렌더링 결과는 `/opt/kafka/config/server.properties`).

   admin 계정을 metadata 에 심는 이유는 브로커 간 인증(INTERNAL 리스너, SCRAM-SHA-512)이 이 자격증명을 쓰기 때문입니다. 포맷 시점에 넣지 않으면 나중에 CLI 로 만들 방법이 없습니다 — 만들려면 그 CLI 가 다시 SCRAM 인증을 통과해야 하기 때문입니다.

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
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@localhost:9093` | `1@10.0.0.11:9093,2@10.0.0.12:9093,3@10.0.0.13:9093` (실 IP) |
| advertised 주소 | `localhost` 고정 | 실 IP (.env 주입) |
| 복제 계수 (RF/min.insync 등) | 전부 1 | 3 / 2 |
| 데이터 볼륨 | named volume | 호스트 경로 bind mount (`/home/ow/kafka/data`) |
| INTERNAL 리스너 | 구조상 유지 (실사용 없음) | 브로커 간 복제 트래픽 |

리스너 3개 구조, SCRAM-SHA-512, keystore/truststore, 컨트롤러 mTLS, hostname verification 은 동일합니다.

## 주의

- `.env` 와 `secrets/`, `certs/` 산출물은 **커밋하지 않습니다** (루트 `.gitignore` 가 제외).
- 앱을 붙일 때는 admin 대신 별도 SCRAM 계정을 만드세요 — [운영 치트시트](../../commands/kafka-operations-cheatsheet.md)와 07 문서 9장 참고.
- 데이터를 초기화하려면 `docker compose down -v` 로 named volume 까지 지운 뒤 4단계(포맷)부터 다시 합니다. 이때 `.env` 의 기존 `KAFKA_CLUSTER_ID` 줄은 지우고 새로 만든 값을 넣습니다 — 볼륨을 지우지 않은 채 4단계를 다시 돌리면 `already formatted` 로 멈춥니다.

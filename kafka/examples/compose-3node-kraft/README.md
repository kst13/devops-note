# Docker Compose 3노드 KRaft 예제

실서버 3대에 KRaft combined 모드 Kafka(SASL_SSL + mTLS)를 올리는 실행 가능한 최소 구성입니다. 설계 배경과 각 설정의 이유는 다음 문서에서 다룹니다.

> 인증·암호화 없이 신뢰된 사설망/학습용으로 빠르게 올리려면 PLAINTEXT 버전을 쓰세요 → [../compose-3node-kraft-plaintext](../compose-3node-kraft-plaintext/README.md). 두 예제의 차이는 **보안뿐**이고 둘 다 KRaft입니다.
> 한 대로 SASL_SSL 흐름만 먼저 익히려면 단일 노드 버전을 쓰세요 → [../compose-1node-kraft](../compose-1node-kraft/README.md). 노드 수만 다르고 보안 구성은 같습니다.

- 개념: [Kafka 기본 개념](../../concepts/01-kafka-basics.md)
- 설계: [실서버 3대 클러스터 설계 방식](../../concepts/06-cluster-design.md)
- 절차: [KRaft 3노드 클러스터 설치 및 설정 방법](../../concepts/07-kraft-cluster-installation.md)

## 구성 파일

```text
docker-compose.yml   3대 공용 (노드 차이는 .env 로만 주입)
.env.example         노드별로 .env 로 복사해 NODE_ID/IP/KAFKA_HOME_DIR/시크릿 지정
certs/README.md      사설 CA + 노드별 keystore/truststore 생성 절차
certs/generate-certs.sh   위 절차를 한 번에 수행하는 스크립트

${KAFKA_HOME_DIR}/          (각 노드 호스트, 저장소 밖) .env 의 KAFKA_HOME_DIR 로 지정
├── data/                   Kafka 로그 디렉터리 (bind mount → /var/lib/kafka/data)
└── secret/                 keystore/truststore 배치 위치 (bind mount → /etc/kafka/secrets, 읽기 전용)
```

## 실행 순서 (요약)

전제: 3대 = `kafka1`(10.0.0.11), `kafka2`(10.0.0.12), `kafka3`(10.0.0.13). 실제 값으로 바꿔 사용합니다.

1. **노드별 .env 작성**

   ```bash
   cp .env.example .env
   # 서버1: KAFKA_NODE_ID=1, ADVERTISED_HOST=10.0.0.11, KAFKA_KEYSTORE_FILE=kafka1.keystore.jks
   # 서버2/3 도 각자 값으로
   # KAFKA_HOME_DIR: 데이터(data/)·인증서(secret/)를 둘 호스트 절대 경로 (3대 동일하게 두는 것을 권장)
   # KAFKA_CLUSTER_ID: 3단계에서 만든 값을 3대에 같게 기록
   ```

2. **호스트 디렉터리 준비 (각 노드)** — 컨테이너 실행 UID(1000)가 읽고 쓸 수 있어야 합니다.

   ```bash
   set -a; . ./.env; set +a
   sudo mkdir -p "${KAFKA_HOME_DIR}/data" "${KAFKA_HOME_DIR}/secret"
   sudo chown -R 1000:1000 "${KAFKA_HOME_DIR}"
   ```

3. **클러스터 ID 생성 (한 번, 3대 공유)** — 출력값을 세 노드 `.env` 의 `KAFKA_CLUSTER_ID` 에 기록합니다.

   ```bash
   docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid
   ```

4. **인증서 생성·배치** — [certs/README.md](certs/README.md) 절차(또는 `certs/generate-certs.sh`)로 CA·노드 keystore·공통 truststore 를 만들고, 각 노드 `${KAFKA_HOME_DIR}/secret/` 에 복사한 뒤 `chmod 600 *.jks`.

5. **각 노드에서 스토리지 포맷 (최초 1회)** — SCRAM admin 계정 부트스트랩 포함. [07 문서 7장](../../concepts/07-kraft-cluster-installation.md) 참고.

6. **각 노드에서 기동**

   ```bash
   docker compose up -d
   docker compose logs -f kafka   # "Kafka Server started" 확인
   ```

7. **검증** — controller quorum(voters 3), RF3 토픽, 1대 정지 후 무중단. 명령은 [운영 치트시트](../../commands/kafka-operations-cheatsheet.md)와 07 문서 8·10·11장 참고.

## 로컬 학습용 (단일 머신 3컨테이너)

> 운영 구성으로 사용하지 않습니다. 설계·명령을 한 대에서 확인하기 위한 축소 구성입니다.

한 호스트에서 3개 컨테이너를 포트만 달리해 띄우려면, 이 compose 를 서비스 3개(`kafka1/2/3`)로 복제하되 `network_mode: host` 를 빼고 브리지 네트워크 + `ports` 매핑으로 되돌린 뒤, 포트를 `9092/9192/9292` 식으로 나누며 `KAFKA_CONTROLLER_QUORUM_VOTERS` 를 컨테이너명 기준으로 맞춥니다. 학습 목적이라 보안을 PLAINTEXT 로 낮춰 시작해도 됩니다(운영에서는 금지). 무손실 동작(RF3, 1대 정지 시 지속)은 단일 머신에서도 동일하게 확인할 수 있습니다.

## 주의

- `.env` 는 **커밋하지 않습니다** (루트 `.gitignore` 가 제외). 인증서(`${KAFKA_HOME_DIR}/secret/`)와 데이터(`${KAFKA_HOME_DIR}/data/`)는 저장소 밖이라 커밋될 일이 없지만, 인증서는 백업 등으로 외부에 복사되지 않게 관리합니다.
- 노드 간 통신은 호스트 IP 를 그대로 사용합니다(DNS/hosts 매핑 불필요). `docker-compose.yml` 의 `KAFKA_CONTROLLER_QUORUM_VOTERS` IP 는 예시이므로 실제 서버 IP 로 바꿉니다. IP 로 광고·접속하므로 인증서 SAN 에 각 노드 IP 가 반드시 포함되어야 합니다.
- `network_mode: host` 는 리눅스 전용입니다. 컨테이너가 호스트의 9092/9093/9094 에 직접 바인드되므로 해당 포트가 비어 있어야 하고, 방화벽에서 노드 간 9092·9093, 앱 대역의 9094 접근을 허용해야 합니다.
- 이미지 태그 `apache/kafka:4.0.0` 은 예시 고정 버전입니다. 실제 도입 시 사용할 패치 버전을 확인해 고정하세요.

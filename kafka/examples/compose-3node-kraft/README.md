# Docker Compose 3노드 KRaft 예제

실서버 3대에 KRaft combined 모드 Kafka(SASL_SSL + mTLS)를 올리는 실행 가능한 최소 구성입니다. 설계 배경과 각 설정의 이유는 다음 문서에서 다룹니다.

- 개념: [Kafka 기본 개념](../../concepts/01-kafka-basics.md)
- 설계: [실서버 3대 클러스터 설계 방식](../../concepts/03-cluster-design.md)
- 절차: [KRaft 3노드 클러스터 설치 및 설정 방법](../../concepts/04-kraft-cluster-installation.md)

## 구성 파일

```text
docker-compose.yml   3대 공용 (노드 차이는 .env 로만 주입)
.env.example         노드별로 .env 로 복사해 NODE_ID/IP/시크릿 지정
certs/README.md      사설 CA + 노드별 keystore/truststore 생성 절차
secrets/             (직접 생성) keystore/truststore 배치 위치, .gitignore 대상
```

## 실행 순서 (요약)

전제: 3대 = `kafka1`(10.0.0.11), `kafka2`(10.0.0.12), `kafka3`(10.0.0.13). 실제 값으로 바꿔 사용합니다.

1. **인증서 생성** — [certs/README.md](certs/README.md) 절차로 CA·노드 keystore·공통 truststore 를 만들고, 각 노드 `secrets/` 에 배치.

2. **노드별 .env 작성**

   ```bash
   cp .env.example .env
   # 서버1: KAFKA_NODE_ID=1, ADVERTISED_HOST=10.0.0.11, KAFKA_KEYSTORE_FILE=kafka1.keystore.jks
   # 서버2/3 도 각자 값으로
   ```

3. **클러스터 ID 생성 (한 번, 3대 공유)**

   ```bash
   KAFKA_CLUSTER_ID=$(docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid)
   echo "$KAFKA_CLUSTER_ID"   # 세 노드에 같은 값 전달
   ```

4. **각 노드에서 스토리지 포맷 (최초 1회)** — SCRAM admin 계정 부트스트랩 포함. 03 문서 7장 참고.

5. **각 노드에서 기동**

   ```bash
   docker compose up -d
   docker compose logs -f kafka   # "Kafka Server started" 확인
   ```

6. **검증** — controller quorum(voters 3), RF3 토픽, 1대 정지 후 무중단. 명령은 [운영 치트시트](../../commands/kafka-operations-cheatsheet.md)와 03 문서 8·10·11장 참고.

## 로컬 학습용 (단일 머신 3컨테이너)

> 운영 구성으로 사용하지 않습니다. 설계·명령을 한 대에서 확인하기 위한 축소 구성입니다.

한 호스트에서 3개 컨테이너를 포트만 달리해 띄우려면, 이 compose 를 서비스 3개(`kafka1/2/3`)로 복제하고 포트를 `9092/9192/9292` 식으로 나누며 `KAFKA_CONTROLLER_QUORUM_VOTERS` 를 컨테이너명 기준으로 맞춥니다. 학습 목적이라 보안을 PLAINTEXT 로 낮춰 시작해도 됩니다(운영에서는 금지). 무손실 동작(RF3, 1대 정지 시 지속)은 단일 머신에서도 동일하게 확인할 수 있습니다.

## 주의

- `.env` 와 `secrets/` 는 **커밋하지 않습니다.** 저장소 루트 `.gitignore` 에 추가하세요.
- `docker-compose.yml` 의 `extra_hosts` IP 는 예시입니다. DNS 가 있으면 제거하고, 없으면 실 IP 로 바꿉니다.
- 이미지 태그 `apache/kafka:4.0.0` 은 예시 고정 버전입니다. 실제 도입 시 사용할 패치 버전을 확인해 고정하세요.

# 단일 노드 KRaft SASL_SSL 예제 설계

- 날짜: 2026-08-09
- 상태: 승인됨 (사용자와 브레인스토밍으로 확정)
- 위치: `kafka/examples/compose-1node-kraft/`

## 목적

로컬 개발 PC에서 SASL_SSL(TLS 암호화 + SASL/SCRAM 인증) 흐름을 학습·검증하기 위한 단일 노드 Kafka 예제. 기존 [compose-3node-kraft](../../../kafka/examples/compose-3node-kraft/README.md)와 "노드 수만 다름"으로 짝을 이루도록, plaintext 예제 ↔ SASL_SSL 예제가 "보안만 다름"으로 짝을 이루는 기존 구조와 일관성을 맞춘다.

**운영용이 아니다.** advertised 주소를 `localhost`로 고정하고 복제 계수를 1로 낮춘 학습 전용 구성이다.

## 결정 사항

브레인스토밍에서 두 안을 비교해 **1안(3노드와 동일한 리스너 구조)** 로 확정했다.

- 1안 (채택): INTERNAL/CONTROLLER/CLIENT 리스너 3개를 그대로 유지. 3노드 예제와 나란히 놓고 비교 학습이 가능하고, 확장 시 구조 변경이 없다.
- 2안 (기각): CONTROLLER + CLIENT 2개로 줄이고 `inter.broker.listener.name`을 CLIENT로 지목. 설정은 짧아지지만 3노드 예제와 구조가 달라져 비교 학습에 불리.

단일 노드에서 INTERNAL 리스너는 실사용되지 않지만(브로커 간 트래픽이 없음), `inter.broker.listener.name`은 어떤 리스너든 가리켜야 하므로 3노드와의 대칭성을 위해 전용 리스너를 유지한다. 이 사실은 compose 주석으로 명시한다.

## 산출물

```text
kafka/examples/compose-1node-kraft/
├── README.md            # 용도, 빠른 시작, 3노드 예제와의 차이·상호 링크
├── docker-compose.yml   # 리스너 3개 구조 유지, 단일 노드 값
├── .env.example         # 시크릿 자리 표시 (__SET_ME__)
└── certs/README.md      # localhost SAN 인증서 생성 절차
```

## docker-compose.yml 설계

3노드 예제의 파일을 기준으로 하고, 아래 항목만 바꾼다. 보안 관련 구조(SASL/SCRAM-SHA-512, keystore/truststore, 컨트롤러 mTLS `ssl.client.auth=required`, hostname verification `https`)는 전부 동일하게 유지한다.

| 항목 | 3노드 | 1노드 |
|---|---|---|
| `KAFKA_NODE_ID` | .env 주입 (1~3) | `1` 고정 |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka1:9093,2@kafka2:9093,3@kafka3:9093` | `1@localhost:9093` |
| `ADVERTISED_HOST` | 실 IP (.env 주입) | `localhost` 고정 |
| `KAFKA_KEYSTORE_FILE` | 노드별 (.env 주입) | `kafka.keystore.jks` 고정 |
| RF / min.insync / offsets RF / txn RF / txn min ISR | 3 / 2 / 3 / 3 / 2 | 전부 1 |
| 데이터 볼륨 | 호스트 경로 `/data/kafka` | named volume (`kafka-data`) |
| `extra_hosts` | kafka1~3 매핑 | 제거 |

`.env`에는 시크릿(keystore/truststore/key 비밀번호, inter-broker SCRAM 계정)만 남는다. 노드 식별 값은 파일에 고정한다.

## 인증서 설계 (certs/README.md)

3노드 절차의 축소판. 학습용 사설 CA 기준.

- 사설 CA 1개 생성 → 공통 truststore 생성 → keystore 1개 발급.
- SAN: `DNS:localhost, IP:127.0.0.1`. advertised 주소(`localhost`)와 일치해 hostname verification을 켠 채로 동작한다.
- 컨트롤러 리스너가 mTLS이므로 EKU에 serverAuth·clientAuth를 함께 부여한다 (3노드와 동일).
- 산출물(`*.jks`, `*.key`, `*.crt`)과 비밀번호는 커밋 금지, `secrets/`는 `.gitignore` 대상 — 기존 예제와 같은 규칙.

## README 빠른 시작 흐름

1. 인증서 생성 (`certs/README.md`)
2. `.env` 작성 (`cp .env.example .env` 후 비밀번호 채움)
3. 클러스터 ID 생성 + 스토리지 포맷 (최초 1회, `--add-scram`으로 admin 부트스트랩)
4. `docker compose up -d`, 로그로 기동 확인
5. 클라이언트용 SCRAM 계정 생성
6. console producer/consumer로 SASL_SSL 접속 검증 (`openssl s_client` TLS 확인 포함)

3노드 README와 상호 링크: 이 README에 "클러스터로 확장하려면 → compose-3node-kraft", 3노드 README에 "한 대로 먼저 익히려면 → compose-1node-kraft" 안내를 추가한다.

## 웹 사이트 반영

문서 추가 후 `web/`에서 `npm run sync-content`로 `content.generated.json`을 재생성하고 커밋한다. `npm run lint && npm test` 통과를 확인한다. (examples 카테고리 README는 sync 대상이므로 자동으로 사이트에 노출된다.)

## 비범위

- ACL(권한 제어) 설정은 다루지 않는다 — [10-security-tls-and-auth](../../../kafka/concepts/10-security-tls-and-auth.md) 참조로 갈음.
- 운영 배포(실 IP, Secret Manager 연동, 모니터링)는 3노드 예제의 영역.
- 기존 3노드 예제의 구조 변경 없음 (README 상호 링크 한 줄 추가만).

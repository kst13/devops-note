# Kafka 홈랩 원클릭 구축 (setup.sh)

집 PC 한 대에 사내 클러스터와 같은 보안 흐름을 재현하는 스크립트입니다. [1노드 예제](../compose-1node-kraft/README.md)의 수동 절차에 더해, 실서버 구축 때 수행한 인가·계정·Schema Registry 단계까지 한 번에 자동화했습니다.

```bash
./setup.sh                # 기본 ~/kafka-home-lab 에 구축
./setup.sh /원하는/경로    # 위치 지정
```

## 스크립트가 하는 일

1. 사전 점검 — docker(compose v2)·openssl 확인
2. 자격증명 생성 — admin/schema-registry/app 비밀번호와 클러스터 ID를 `.env`에 기록 (재실행 시 재사용)
3. 인증서 — 사설 CA + 노드 인증서(keystore 는 PKCS12, truststore 는 CA PEM 그대로). keytool 없이 openssl만 사용. SAN에 `localhost`(호스트 접속)와 `kafka`(컨테이너 간 접속)를 함께 넣음
4. compose 생성 — 운영처럼 **브로커(`kafka/`)와 Schema Registry(`schema-registry/`)를 별도 compose 프로젝트**로 생성. 브로커는 KRaft 1노드(SASL_SSL + 컨트롤러 mTLS) + **StandardAuthorizer 인가**, SR 은 브로커가 만든 네트워크에 external 로 붙음. `.env` 도 프로젝트별로 분리(SR 쪽에는 admin 비밀번호 없음)
5. 스토리지 포맷(최초 1회, admin SCRAM 부트스트랩 포함) 후 브로커 기동·대기
6. 계정·토픽·ACL — `schema-registry`(_schemas + DescribeConfigs + 그룹), 실습용 `app` 계정(`sandbox.demo` 토픽 + `demo` 그룹)
7. Schema Registry 기동 후 토픽 목록·`/subjects`로 검증, 실습 명령 안내 출력

## 실서버(3노드)와 다른 점

| 항목 | 홈랩 | 실서버 |
| --- | --- | --- |
| 노드 수 / 복제 | 1노드, RF1 (장애 내성 없음) | 3노드, RF3 + `min.insync.replicas=2` |
| 컨테이너용 리스너 | `DOCKER://kafka:9095` 추가 (Schema Registry가 `kafka-home-lab-net` 네트워크에서 접속) | 호스트 네트워크라 불필요 |
| compose 프로젝트 | `kafka/`, `schema-registry/` 두 프로젝트 (운영의 `/opt/kafka`, `/opt/kafka-ecosystem` 배치를 흉내) | 서버별 디렉터리 |
| 인증서 저장소 | keystore PKCS12 + truststore PEM (openssl만으로 생성) | JKS (keytool) |
| 비밀 관리 | `.env` 자동 생성 | 발급·배포 절차 |

보안 구성(SCRAM 인증, TLS, 컨트롤러 mTLS, ACL 인가, `super.users`에 브로커 인증서 주체 포함)은 실서버와 동일한 흐름입니다 — 실서버 구축 때 겪은 `super.users` 오타(단수형) 교착, 호스트명 검증(SAN) 문제를 그대로 반영해 두었습니다.

## 구축 결과 디렉터리

```text
~/kafka-home-lab/
├── .env                # 스크립트 재실행용 마스터 자격증명 (커밋 금지)
├── secrets/            # CA·keystore·계정 properties (두 프로젝트가 ../secrets 로 공유)
├── kafka/              # 브로커 compose 프로젝트 — 네트워크 kafka-home-lab-net 소유
│   ├── docker-compose.yml
│   └── .env            # KAFKA_CLUSTER_ID, ADMIN_PASSWORD, STORE_PASSWORD
└── schema-registry/    # SR compose 프로젝트 — external 네트워크, depends_on 없음
    ├── docker-compose.yml
    └── .env            # SR_PASSWORD
```

브로커와 SR 이 서로 다른 프로젝트라 한쪽에서 `docker compose down` 을 해도 다른 쪽은 살아 있습니다 (브로커 쪽 `down` 은 SR 이 쓰고 있는 네트워크를 "still in use" 로 남겨 두고 정상 종료합니다). 실측한 동작은 다음과 같습니다.

- 브로커가 멈춰도 **이미 떠 있는 SR 은 재시작하지 않고** 연결이 끊겼다가 브로커가 돌아오면 수 초 내 재연결합니다.
- **브로커 없이 SR 을 새로 기동하면** 진입점의 kafka-ready 확인에서 바로 종료하고 `restart: unless-stopped` 로 반복 재시도하다가, 브로커가 뜨면 30초 내에 정상화됩니다 — 운영에서 SR 서버만 먼저 재부팅됐을 때와 같은 동작입니다.

```bash
cd ~/kafka-home-lab/schema-registry && docker compose restart   # SR 만 재시작, 브로커 무영향
cd ~/kafka-home-lab/kafka && docker compose logs -f kafka        # 브로커 로그
```

## 정리

```bash
cd ~/kafka-home-lab
docker compose --project-directory schema-registry down   # SR 먼저 (네트워크 사용자)
docker compose --project-directory kafka down -v          # 브로커·데이터·네트워크 삭제
rm -rf ~/kafka-home-lab                                   # 인증서·설정까지 완전 삭제
```

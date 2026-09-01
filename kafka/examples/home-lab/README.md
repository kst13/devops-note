# Kafka 홈랩 원클릭 구축 (setup.sh)

집 PC 한 대에 사내 클러스터와 같은 보안 흐름을 재현하는 스크립트입니다. [1노드 예제](../compose-1node-kraft/README.md)의 수동 절차에 더해, 실서버 구축 때 수행한 인가·계정·Schema Registry 단계까지 한 번에 자동화했습니다.

```bash
./setup.sh                # 기본 ~/kafka-home-lab 에 구축
./setup.sh /원하는/경로    # 위치 지정
```

## 스크립트가 하는 일

1. 사전 점검 — docker(compose v2)·openssl 확인
2. 자격증명 생성 — admin/schema-registry/app 비밀번호와 클러스터 ID를 `.env`에 기록 (재실행 시 재사용)
3. 인증서 — 사설 CA + 노드 인증서(PKCS12). keytool 없이 openssl만 사용. SAN에 `localhost`(호스트 접속)와 `kafka`(컨테이너 간 접속)를 함께 넣음
4. compose 생성 — KRaft 1노드(SASL_SSL + 컨트롤러 mTLS) + **StandardAuthorizer 인가** + Schema Registry
5. 스토리지 포맷(최초 1회, admin SCRAM 부트스트랩 포함) 후 브로커 기동·대기
6. 계정·토픽·ACL — `schema-registry`(_schemas + DescribeConfigs + 그룹), 실습용 `app` 계정(`sandbox.demo` 토픽 + `demo` 그룹)
7. Schema Registry 기동 후 토픽 목록·`/subjects`로 검증, 실습 명령 안내 출력

## 실서버(3노드)와 다른 점

| 항목 | 홈랩 | 실서버 |
| --- | --- | --- |
| 노드 수 / 복제 | 1노드, RF1 (장애 내성 없음) | 3노드, RF3 + `min.insync.replicas=2` |
| 컨테이너용 리스너 | `DOCKER://kafka:9095` 추가 (Schema Registry가 같은 compose 네트워크에서 접속) | 호스트 네트워크라 불필요 |
| 인증서 저장소 | PKCS12 (openssl만으로 생성) | JKS (keytool) |
| 비밀 관리 | `.env` 자동 생성 | 발급·배포 절차 |

보안 구성(SCRAM 인증, TLS, 컨트롤러 mTLS, ACL 인가, `super.users`에 브로커 인증서 주체 포함)은 실서버와 동일한 흐름입니다 — 실서버 구축 때 겪은 `super.users` 오타(단수형) 교착, 호스트명 검증(SAN) 문제를 그대로 반영해 두었습니다.

## 정리

```bash
cd ~/kafka-home-lab
docker compose down -v          # 컨테이너·데이터 삭제
rm -rf ~/kafka-home-lab         # 인증서·설정까지 완전 삭제
```

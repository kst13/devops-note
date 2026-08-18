# Kafka

Apache Kafka의 개념, 클러스터 설계 기준, 설치 절차, 운영 명령을 정리하는 디렉터리입니다.

Kafka는 서비스 간 비동기 메시징, 이벤트 소싱, 로그 수집, 스트림 처리 등에 쓰이지만, 용도에 따라 복제·ISR·보안 설정 기준이 크게 달라집니다. 이 문서는 단순 실행 방법보다 **MSA 환경에서 메시지 무손실과 장애 조치를 위해 어떤 설정을 왜 선택해야 하는지**를 함께 남기는 것을 목표로 합니다.

## 정리 원칙

- 개념은 짧게 정의하고, 직접 확인할 수 있는 명령어 또는 설정 예시를 함께 적습니다.
- 트러블슈팅은 증상, 원인, 확인 방법, 해결 방법 순서로 기록합니다.
- 실습 예제는 재현 가능한 최소 구성으로 유지합니다.
- 새로 알게 된 명령어는 `commands/`에 모아 나중에 빠르게 찾아볼 수 있게 합니다.
- 설정값은 무손실, 처리량, 보관 기간처럼 사용 목적을 먼저 밝히고 기록합니다.

## 문서 구조

```text
concepts/          Kafka 핵심 개념과 클러스터 설계 정리
troubleshooting/   자주 만나는 문제와 해결 기록
commands/          Kafka CLI와 운영 명령어 치트시트
examples/          직접 실행해볼 수 있는 예제
```

> **Kafka를 사용하는 개발자라면 [Kafka 사용 원칙(개발자용)](concepts/12-usage-principles.md)을 먼저 읽으세요.** 안전하게 쓰기 위한 규칙과 접속 템플릿이 정리돼 있습니다.

## 추천 학습 순서

1. [Kafka 핵심 개념: 토픽, 파티션, 브로커, 컨트롤러](concepts/01-kafka-basics.md)
2. [Producer와 복제](concepts/02-producer-and-replication.md)
3. [Consumer와 Consumer Group](concepts/03-consumer-and-consumer-group.md)
4. [브로커 내부 구조](concepts/04-broker-internals.md)
5. [KRaft 등장 배경과 ZooKeeper 대비 장단점](concepts/05-kraft-vs-zookeeper.md)
6. [실서버 3대 클러스터 설계 방식](concepts/06-cluster-design.md)
7. [KRaft 3노드 클러스터 설치 및 설정 방법](concepts/07-kraft-cluster-installation.md)
8. [Kafka 설정 레퍼런스](concepts/08-configuration-reference.md)
9. [개념 정리 Q&A (실무 관점)](concepts/09-concepts-qna.md)
10. [보안: 암호화(TLS)와 인증(SASL·mTLS)](concepts/10-security-tls-and-auth.md)
11. [브로커·컨트롤러·주키퍼 프로세스 구조와 failover 원리](concepts/11-broker-controller-zookeeper-and-failover.md)
12. [Kafka 사용 원칙 (개발자용)](concepts/12-usage-principles.md)
13. [메시지 키와 순서 보장: 파티션, 전역 순서, 선착순 처리](concepts/13-message-key-and-ordering.md)

실행 가능한 예제(둘 다 KRaft, 보안만 다름):

- [SASL_SSL + mTLS 버전](examples/compose-3node-kraft/README.md) — 운영 권장
- [PLAINTEXT 버전](examples/compose-3node-kraft-plaintext/README.md) — 사설망/학습용

## 참고 기준

이 디렉터리는 2026-07-21 기준 Kafka 4.x(KRaft 전용) 공식 문서를 우선 참고합니다. 운영 환경에서는 사용 중인 Kafka 버전, 배포 방식, 클라이언트 라이브러리 버전을 함께 확인해야 합니다.

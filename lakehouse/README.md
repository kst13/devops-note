# Lakehouse

Kafka·MinIO·Iceberg·Trino로 구성하는 데이터 레이크하우스 계층을 정리하는 디렉터리입니다. 회원·주문·채팅·결제처럼 서비스별로 흩어진 데이터를 장기 보관하고, SQL 하나로 통합 조회하는 것이 목표입니다.

Lakehouse는 객체 저장소(MinIO)에 파일을 쌓고, 테이블 포맷(Iceberg)으로 그 파일들을 하나의 테이블로 관리하며, SQL 엔진(Trino)으로 조회하는 구조입니다. 운영 DB를 대체하지 않습니다. 업무 처리와 빠른 화면 조회는 DB가 맡고, 장기 이력과 분석은 Lakehouse가 맡습니다.

## 정리 원칙

- 구성요소는 "어떤 질문에 답하는가"로 정의하고, 직접 확인할 수 있는 SQL·명령·설정을 함께 적습니다.
- 설정값은 무손실, 반영 지연, 보관 기간처럼 사용 목적을 먼저 밝히고 기록합니다.
- 실습 예제는 기존 [Kafka 홈랩](../kafka/examples/home-lab/README.md)을 재사용하고, Lakehouse 부분만 별도 compose 프로젝트로 붙입니다.
- 기업 사례는 공개 자료 기준으로 적고, 발표 당시 구성과 우리 구성의 차이를 명시합니다.

## 문서 구조

```text
concepts/          Lakehouse 개념, 구성요소, 흐름, DB와의 역할 분담, 운영 원칙
examples/          로컬 PoC (MinIO + Iceberg REST Catalog + Trino + Kafka Connect Iceberg Sink)
```

## 추천 학습 순서

1. [Lakehouse란 무엇인가](concepts/01-what-is-lakehouse.md) — Data Lake와의 차이, 세 층 구조, 이번 구축의 목적
2. [구성요소 정의와 역할](concepts/02-components.md) — Kafka, Writer, MinIO, Parquet, Iceberg, Catalog, Trino
3. [데이터는 어떻게 저장되고 조회되는가](concepts/03-write-and-read-flow.md) — 이벤트 수집 → 파일 생성 → 테이블 커밋 → SQL 조회, 이력 테이블과 상태 테이블
4. [운영 DB와의 역할 분담과 플랫폼 계층](concepts/04-role-split-and-platform-layers.md) — Serving·Lakehouse·Search, 서비스별 활용
5. [콜드 데이터 이관](concepts/05-cold-data-migration.md) — 이관 기준, 절차, 삭제 전 확인, 삭제 정책 구분
6. [데이터 정합성과 운영 원칙](concepts/06-consistency-and-operations.md) — Outbox·CDC, 이벤트 규약, 재처리, 유지보수, 운영 지표
7. [기업 사례에서 배울 점](concepts/07-industry-cases.md) — Uber DBEvents, SK텔레콤 Trino Summit, Netflix Data Bridge
8. [단계별 구축과 PoC 계획](concepts/08-rollout-plan.md) — 활용 사례 단위의 단계, 완료 기준, 의사결정 항목
9. [구축 순서: 로컬 PoC부터 실서버까지](concepts/09-build-sequence.md) — MinIO → 카탈로그 → Trino → 첫 테이블 → Connect → 초기 적재 → 검증 → 운영 작업 → 조회 제공, 단계별 명령·설정·완료 기준

실행 가능한 예제:

- [로컬 PoC](examples/local-poc/README.md) — 1단계 MinIO + Catalog + Trino, 2단계 Kafka Connect Iceberg Sink를 홈랩 Kafka에 연결

관련 문서:

- [MinIO](../minio/README.md) — 객체 저장소 배포 형태, 구축 구성안, 파일 애플리케이션 이전
- [Kafka](../kafka/README.md) — 이벤트 수집 경로의 브로커·프로듀서·컨슈머

## 참고 기준

2026-09-13 기준 Apache Iceberg 1.10, Trino 483, Iceberg Kafka Connect Sink 1.9.2 공식 문서를 우선 참고합니다. 발표자료 초안은 `docs/lakehouse-layer-proposal.html`에 있습니다.

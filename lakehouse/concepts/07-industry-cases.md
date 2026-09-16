# 기업 사례에서 배울 점

> 공개 자료에서 확인되는 것은 "업무 DB를 유지하면서 변경을 분석용 저장소로 전달"하거나 "기존 데이터 레이크를 Iceberg로 개선"한 사례입니다. 각 사례는 발표 당시 구성이며, 우리와 같은 제품 조합은 아닙니다. "DB의 모든 기능을 MinIO로 옮기는 구조"는 어느 사례에도 없습니다.

## 1. 요약

| 기업 · 출처 | 목적 | 기술과 접근 (발표 당시) | 우리 프로젝트에 적용할 점 |
| --- | --- | --- | --- |
| Uber · DBEvents 블로그 (2019) | 여러 업무 DB의 데이터를 공통 방식으로 데이터 레이크에 수집 | MySQL binlog → StorageTapper → Kafka → Marmaray → Hudi(HDFS). 초기 적재와 변경분 수집 분리 | 초기 적재 + 변경분 수집을 함께 설계. "장애 후 어디서부터 다시 읽나"를 검증 항목에 |
| SK텔레콤 · Trino Summit 2022 | Hive 기반 분석 환경의 확장 문제 해결과 분석 조회 개선 | 온프레미스 Hadoop/HDFS. Iceberg 파일 통계로 불필요한 파일 읽기 감소 | 실제 조회 조건 기준으로 파티션·파일 배치 설계 |
| Netflix · AWS re:Invent 2023 NFX306 | 약 1EB 레이크를 Hive에서 Iceberg 전용으로 전환 | 300PB 이동 최소화 이관, 상태 기계 기반 도구, 보안 테이블, Iceberg REST 카탈로그 개발. S3 · Iceberg · Spark, Trino | 비슷한 역할 분리. S3→MinIO 교체 시 운영·복구·성능은 PoC로 확인 |

## 2. Uber — 업무 DB의 변경 데이터를 분석 플랫폼으로

기준은 2019년 공개한 DBEvents 구조입니다. 당시 기술은 Kafka·StorageTapper·Marmaray·Hudi·HDFS이며, 이후의 Flink 개선 사례나 우리가 검토하는 MinIO·Iceberg 구성과는 구분합니다.

### 왜 이런 구조가 필요했나

기존에는 테이블 전체를 반복해서 읽어 분석 저장소에 적재했습니다. 데이터가 커질수록 복사 시간이 늘고 업무 DB에도 부하가 생겼습니다. 그래서 **초기 적재(Bootstrap)와 변경분 적재(CDC)로 나눴습니다.** 주문이 1억 건이고 오늘 바뀐 주문이 10만 건이라면, 초기 데이터는 한 번 적재하고 이후 변경분만 반영하는 방식입니다(설명용 예시).

### 구조

```text
MySQL Primary (업무 요청 처리)
   │  ② binlog 스트리밍 (변경 기록)
   ▼
MySQL 복제본 ── ① 초기 스냅샷 (Primary 부하 회피, 속도 제한) ──▶ StorageTapper (변경 → Avro 메시지)
                                                                    │
                                                                    ▼
                                                                  Kafka
                                                                    │
                                                                    ▼
                                    오류 테이블 ◀── 오류 ── Marmaray (변환 · 적재 · 체크포인트)
                                    (원본·오류 보관,               │ 정상 → upsert
                                     수정 후 재투입)                ▼
                                                         Hudi 분석 테이블 (HDFS)
                                                         키 기준 갱신: 주문 A 결제대기 → 결제완료
```

- **초기 스냅샷은 복제본에서.** StorageTapper가 MySQL 복제본에서 읽어 Primary 부하를 피하고 읽기 속도를 제한합니다. 큰 테이블은 작업을 나눠 처리합니다.
- **변경은 binlog에서.** 변경 내용을 Avro 메시지로 바꿔 Kafka로 보내고, Marmaray가 읽어 Hudi 테이블에 upsert합니다. 애플리케이션이 분석 저장소에 직접 쓰지 않고, DB에 반영된 변경을 수집 경로가 따라갑니다.
- **표준 메타데이터.** 행 식별자, 증가하는 변경 버전, 삭제 여부를 메시지에 붙입니다. 버전 11을 적용한 뒤 버전 10이 늦게 와도 이전 상태로 돌아가지 않게 합니다.
- **오류 격리.** 변환 실패 데이터는 오류 테이블에 원본과 함께 보관하고, 원인을 고친 뒤 다시 파이프라인에 넣습니다.
- **체크포인트.** 마지막 성공 위치를 저장하고 작업이 성공했을 때만 갱신합니다. 실패하면 이전 위치부터 다시 시작합니다. 체크포인트만으로 중복 방지가 완성되지는 않습니다. 저장은 됐는데 체크포인트 저장 전에 죽을 수 있으므로, 재실행 시 같은 데이터를 안전하게 처리하는 규칙이 함께 필요합니다.

또 다른 공개 글에서는 Kafka → 데이터 레이크 수집 경로를 Flink 기반으로 개선했습니다(테이블 포맷은 Hudi). 파티션별 처리 편중과, Flink 체크포인트·저장소 커밋의 불일치로 생길 수 있는 누락·중복 문제를 다룹니다.

### 콜드 데이터 이관에 적용할 때의 차이

Uber 사례는 업무 DB의 변경을 분석 테이블에 **따라 반영**하는 구조입니다. 변경 메시지를 수집하는 것과 모든 이력을 영구 보존하는 것은 별개입니다. Hudi upsert는 최신 상태를 유지합니다. 이 구조만으로는 "DB에서 지워도 분석 저장소에 남는 아카이브"가 되지 않습니다. DB 정리용 삭제를 CDC가 수집해 분석 테이블에서도 지우기 때문입니다.

우리 프로젝트에서는 초기 적재 + 변경분 수집 + 오류 격리 + 체크포인트 원칙을 가져오되, **장기 보관용 이력 테이블과 삭제 정책을 따로 설계**합니다([05 콜드 데이터 이관](05-cold-data-migration.md) 6장).

## 3. SK텔레콤 — Trino와 Iceberg로 분석 조회 개선

Trino Summit 2022에서 기존 Hive 기반 분석 환경의 확장 문제와 Iceberg 도입 경험을 공개했습니다. 당시 저장 기반은 온프레미스 Hadoop/HDFS였으며 MinIO 사례는 아닙니다.

- Trino의 배치 조회 환경과 임시 분석 조회 환경을 구분했습니다.
- 쿼리 계획, JMX, 시스템 지표, 로그를 수집해 병목을 분석했습니다.
- Hive 메타데이터 접근과 파일 목록 조회 비용을 문제로 확인했습니다.
- Iceberg의 파일 통계와 메타데이터를 활용해 불필요한 파일 읽기를 줄였습니다.
- 파티션을 너무 많이 나누면 쿼리 계획 시간이 늘어나는 절충 관계도 확인했습니다.

```text
Trino SQL ──▶ Iceberg 메타데이터 (파일 위치 · 컬럼 통계) ──▶ 조건에 필요한 파일 선택 ──▶ 저장소에서 선택한 파일만 읽기
```

적용할 점: "Iceberg를 쓰면 빨라진다"로 끝내지 않습니다. 실제 조회 조건을 기준으로 파티션과 파일 배치를 설계합니다. 고객별 조회와 월별 전체 집계는 요구하는 데이터 배치가 다를 수 있습니다. 수치를 인용할 때는 발표 자료 출처를 확인합니다.

## 4. Netflix — Hive에서 Iceberg 전용 데이터 레이크로

Netflix는 Iceberg를 만들어 2018년 Apache에 기증한 곳입니다. AWS re:Invent 2023 NFX306 발표는 약 1EB 규모 데이터 레이크를 Hive에서 Iceberg 전용으로 옮긴 과정을 다룹니다.

- 남아 있던 Hive 테이블 300PB를 데이터 이동을 최소화하는 방식으로 이관했습니다. 이관 도구는 상태 기계로 만들어 단계별 진행과 재시도를 관리했습니다.
- 보안 Iceberg 테이블과 자체 Iceberg REST 카탈로그, 메타데이터·테이블 관리 서비스를 개발했습니다.
- 이점으로 시점 조회, 스키마 진화, 사용자 마찰 감소를 들었습니다.

| 역할 | Netflix (2023 발표) | 이 문서 |
| --- | --- | --- |
| 객체 저장 | Amazon S3 | MinIO (S3 호환) |
| 테이블 관리 | Apache Iceberg (전용) | Apache Iceberg |
| 카탈로그 | 자체 Iceberg REST 카탈로그 | REST(PoC) → Postgres JDBC → 필요 시 REST |
| 처리·조회 | Spark, Trino | Trino |

적용할 점: 우리 구성은 이와 비슷한 역할 분리를 갖습니다. 다만 S3를 MinIO로 바꾸었을 때 운영·복구·성능까지 동일하다고 볼 수는 없습니다. PoC에서 확인합니다. 참고로 2024년 발표(NFX304 Data Bridge)는 배치 데이터 이동 제어판 소개이며, Iceberg는 여러 목적지 중 하나로 나옵니다.

## 5. 공식 문서에서 확인한 연결

| 확인할 내용 | 공식 문서에서 확인한 사항 |
| --- | --- |
| Kafka → Iceberg | Apache Iceberg가 Kafka Connect Sink 제공 |
| Iceberg 테이블 등록 | Trino가 REST·JDBC 등 여러 Catalog 지원 |
| MinIO 파일 접근 | Trino의 S3 파일 시스템 문서에서 MinIO 호환성 테스트 명시 |
| SQL 조회 | Trino Iceberg 커넥터로 테이블 조회 |

따라서 Kafka → Kafka Connect Iceberg Sink → MinIO + Catalog → Trino 구성은 공식 기능을 조합해 구축할 수 있는 형태입니다. [로컬 PoC](../examples/local-poc/README.md)가 이 조합을 그대로 띄웁니다.

## 관련 문서

- [06 데이터 정합성과 운영 원칙](06-consistency-and-operations.md)
- [08 단계별 구축과 PoC 계획](08-rollout-plan.md)

## 참고한 공개 자료

- [Uber Engineering — DBEvents: A Standardized Framework for Efficiently Ingesting Data into Uber's Apache Hadoop Data Lake](https://www.uber.com/blog/dbevents-ingestion-framework/)
- [Uber — StorageTapper (GitHub)](https://github.com/uber/storagetapper), [Uber — Marmaray (GitHub)](https://github.com/uber/marmaray)
- Trino Summit 2022 — SK Telecom 발표 (trino.io 블로그와 Trino YouTube 채널의 Summit 2022 세션에서 "SK Telecom"으로 검색)
- [AWS re:Invent 2023 — Netflix's journey to an Apache Iceberg-only data lake (NFX306)](https://www.youtube.com/watch?v=jMFMEk8jFu8)
- [Netflix Tech Blog — Data Bridge: How Netflix simplifies data movement (re:Invent 2024 NFX304 관련)](https://netflixtechblog.medium.com/data-bridge-how-netflix-simplifies-data-movement-36d10d91c313)
- [Iceberg — Kafka Connect](https://iceberg.apache.org/docs/latest/kafka-connect/), [Trino — Metastores](https://trino.io/docs/current/object-storage/metastores.html), [Trino — S3 file system](https://trino.io/docs/current/object-storage/file-system-s3.html)

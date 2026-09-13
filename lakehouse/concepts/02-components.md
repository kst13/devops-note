# 구성요소 정의와 역할

> Kafka·MinIO·Iceberg·Trino만으로는 수집부터 조회까지 완성되지 않습니다. Kafka 데이터를 테이블에 기록하는 **Writer**와 테이블 위치를 관리하는 **Catalog**가 더 필요합니다.

## 1. 한눈에 보기

| 구성요소 | 정의 | 이번 플랫폼에서의 역할 | 답하는 질문 |
| --- | --- | --- | --- |
| Kafka | 이벤트를 저장하고 전달하는 플랫폼 | 업무 시스템의 변경을 수집 경로로 전달 | 데이터가 어디에서 들어오나? |
| Writer | 데이터를 옮기거나 처리하는 실행 도구 | Kafka 이벤트를 Iceberg 테이블에 기록 | 누가 테이블에 기록하나? |
| MinIO | S3 호환 객체 저장소 | 데이터 파일과 Iceberg 메타데이터 파일 보관 | 실제 파일은 어디에 있나? |
| Parquet | 컬럼형 데이터 파일 포맷 | 거래·메시지 데이터를 효율적으로 저장 | 파일 내부는 어떤 형식인가? |
| Iceberg | 대규모 분석용 테이블 포맷 | 여러 파일을 하나의 일관된 테이블로 관리 | 어떤 파일들이 한 테이블인가? |
| Iceberg Catalog | 테이블 등록·위치 관리 기능 | 테이블 이름으로 현재 메타데이터를 찾게 함 | 테이블의 현재 메타데이터는 어디? |
| Trino | 분산 SQL 쿼리 엔진 | Iceberg 테이블 조회·조인·집계 | 누가 SQL을 실행하나? |

## 2. Kafka — 수집 경로

업무 시스템에서 발생한 이벤트를 Topic에 기록합니다. Writer는 Consumer로서 어디까지 읽었는지(오프셋)를 추적하며 가져갑니다.

```text
주문 시스템 ──▶ Kafka · order.events
                 Partition 0: offset 40 주문 생성 / 41 결제 완료 / 42 주문 취소
                                        │
                                        ▼
                               Writer (읽은 위치 추적) ──▶ Iceberg 테이블
```

- 보관 기간 안에서는 이벤트를 다시 읽을 수 있습니다. 수집 장애 복구와 적재 로직 재실행에 씁니다.
- 장기 이력 보관은 Kafka가 아니라 Lakehouse가 담당합니다. Kafka 보관 기간은 "재처리 창"으로 정합니다.
- Kafka에 들어온 시각과 Trino에서 조회되는 시각은 다릅니다. Writer의 커밋 주기가 그 차이를 만듭니다.

Kafka 자체는 [Kafka 토픽](../../kafka/README.md)에서 다룹니다.

## 3. Writer — 이벤트를 파일로 만들고 테이블에 반영

Kafka 이벤트가 자동으로 Iceberg 테이블이 되지는 않습니다. 다음 다섯 단계를 수행하는 프로그램이 필요합니다.

1. Kafka 이벤트를 읽습니다.
2. 이벤트를 테이블 컬럼에 맞게 변환합니다.
3. 일정량을 모아 데이터 파일을 만듭니다.
4. MinIO에 파일을 저장합니다.
5. Iceberg 테이블에 변경 사항을 커밋합니다.

| 후보 | 적합한 용도 |
| --- | --- |
| Kafka Connect + Iceberg Sink | 비교적 단순한 이벤트 적재. Apache Iceberg가 공식 커넥터 제공. **첫 선택** |
| Flink + Iceberg Sink | 스트림 조인, 상태 기반 처리, 이벤트 시간 집계 |

커밋은 새 파일들을 테이블의 정식 데이터로 확정하는 작업입니다. MinIO에 파일을 올리는 것만으로는 테이블에 반영되지 않습니다. 이벤트를 묶는 시간이 길수록 파일은 효율적이지만 조회 반영은 늦어집니다. 적재 주기는 이 균형으로 정합니다.

## 4. MinIO — 실제 파일을 보관하는 저장소

S3 호환 API로 파일을 저장하고 읽는 객체 저장소입니다.

| 용어 | 의미 | 예시 |
| --- | --- | --- |
| Bucket | 객체를 보관하는 최상위 구획 | `lakehouse` |
| Object | 저장된 파일 단위의 데이터 | Parquet 파일 하나 |
| Key | Bucket 안에서 객체를 식별하는 이름 | `warehouse/commerce/order_events/data/00000-....parquet` |
| Endpoint | 저장소에 접근하는 주소 | `http://minio:9000` |

```text
lakehouse/                          ← Bucket
└── warehouse/commerce/order_events/
    ├── data/       *.parquet        ← 실제 데이터
    └── metadata/   *.metadata.json, *.avro (manifest)   ← Iceberg 메타데이터
```

폴더처럼 보이지만 실제로는 `/`가 포함된 Key입니다. MinIO는 파일을 보관하고 반환할 뿐, 고객별 집계 같은 계산은 Trino가 합니다. 배포 형태와 구축 구성은 [MinIO 토픽](../../minio/README.md) 참고.

## 5. Parquet — 데이터 파일의 내부 형식

데이터를 **컬럼 중심**으로 저장하는 파일 포맷입니다.

| 주문 | 고객 | 금액 |
| --- | --- | --- |
| A | Kim | 10,000 |
| B | Lee | 20,000 |
| C | Kim | 30,000 |

행 단위 파일은 세 행을 통째로 읽어야 합니다. Parquet는 `[A,B,C]`, `[Kim,Lee,Kim]`, `[10000,20000,30000]`처럼 컬럼별로 저장하므로 "금액 합계"는 금액 컬럼만 읽습니다. 컬럼이 30개인 테이블에서 2개만 쓰는 분석 쿼리가 빨라지는 이유입니다. 같은 타입이 연속되어 압축률도 높습니다.

| 구분 | 관리 대상 |
| --- | --- |
| Parquet | 파일 **하나 안의** 데이터 구조 |
| Iceberg | 테이블을 구성하는 **파일들**과 테이블 상태 |

Trino의 Iceberg 커넥터는 Parquet 외에 ORC와 Avro도 지원합니다. 기본은 Parquet입니다.

## 6. Iceberg — 파일들을 하나의 테이블로 관리하는 규칙

| 기능 | 의미 |
| --- | --- |
| 스키마 관리 | 컬럼 이름과 타입. 컬럼 추가·이름 변경을 파일 재작성 없이 지원 |
| 스냅샷 | 특정 시점의 테이블 상태(파일 목록) 기록 |
| 원자적 커밋 | 쓰기 결과가 완료된 상태로만 보임. 적재 중인 파일은 조회에 안 잡힘 |
| 파티션 진화 | 데이터 분할 방식을 나중에 바꿀 수 있음 |
| 파일 추적 | 조회에 쓸 데이터 파일과 통계를 메타데이터로 관리 |

```text
스냅샷 1 (첫 적재)  ──▶ 파일 A (주문 1~100), 파일 B (101~200)
스냅샷 2 (추가 적재) ──▶ 파일 A, 파일 B, 파일 C (201~250)   ← A·B는 복사가 아니라 참조
현재 테이블 상태 = 스냅샷 2
```

스냅샷은 전체 데이터를 복사한 백업이 아닙니다. 상태를 구성하는 파일 목록을 메타데이터로 가리킵니다. 과거 스냅샷과 파일이 남아 있는 범위에서 과거 시점 조회가 됩니다. 스냅샷을 정리하면 그 시점은 더 볼 수 없습니다.

## 7. Iceberg Catalog — 테이블 이름과 현재 메타데이터를 연결

Trino가 `commerce.order_events`를 조회하려면 그 테이블의 현재 메타데이터 파일이 어디 있는지 알아야 합니다. Catalog가 이 연결을 관리합니다.

```text
테이블 이름 commerce.order_events
   │
   ▼
Catalog: 현재 메타데이터 위치 = s3://lakehouse/warehouse/.../metadata/00012-....metadata.json
   │
   ▼
Iceberg 메타데이터: 현재 스냅샷 · 스키마 · 매니페스트 목록
   │
   ▼
매니페스트: 데이터 파일 위치와 컬럼 통계 → 조건에 맞는 파일만 선택
   │
   ▼
MinIO의 Parquet 파일 읽기
```

| 구현 | 특징 |
| --- | --- |
| REST Catalog | 통신 규약. 규약을 제공하는 서버(구현체)를 따로 선택. 엔진 간 호환이 가장 넓음. **첫 선택** |
| JDBC Catalog | 관계형 DB 테이블에 등록 정보 저장. 구성 단순 |
| Hive Metastore | 기존 Hadoop 생태계와 호환 |

두 가지 원칙이 있습니다.

- **Writer와 Trino는 같은 Catalog를 써야 합니다.** 그래야 Writer가 커밋한 상태를 Trino가 찾습니다.
- **Catalog의 등록 정보와 저장소의 파일을 함께 백업**해야 합니다. 한쪽만 있으면 복구가 안 됩니다.

용어가 비슷한 두 개념을 구분합니다. `lakehouse.commerce.order_events`에서 `lakehouse`는 **Trino Catalog**(연결 설정 이름)이고, 그 설정 안에서 **Iceberg Catalog**(REST 등)를 지정합니다.

## 8. Trino — SQL을 나누어 실행하는 계산 계층

```text
사용자 · BI · Query API
        │ SQL
        ▼
   Coordinator: SQL 해석 · 실행 계획 · 작업 분배 · 결과 취합
     ├── Worker 1: 파일 A·B 읽기 · 부분 집계 ──┐
     └── Worker 2: 파일 C·D 읽기 · 부분 집계 ──┴─▶ 최종 결과
```

```sql
SELECT customer_id, SUM(amount) AS total_amount
FROM lakehouse.commerce.order_events
WHERE event_type = 'ORDER_CREATED'
GROUP BY customer_id;
```

파일은 MinIO에 있고 계산은 Trino에서 합니다. 저장 용량과 연산 자원을 따로 확장할 수 있습니다. 조회 성능은 파일 크기, 데이터 배치, 필터 조건, 동시 쿼리 수에 좌우되므로 실제 쿼리로 검증해야 합니다.

## 관련 문서

- [03 데이터는 어떻게 저장되고 조회되는가](03-write-and-read-flow.md)
- [로컬 PoC](../examples/local-poc/README.md) — 위 구성요소를 실제로 띄워 보는 예제

## 참고한 공식 문서

- [Iceberg — Kafka Connect](https://iceberg.apache.org/docs/latest/kafka-connect/)
- [Iceberg — Table Spec](https://iceberg.apache.org/spec/)
- [Trino — Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Trino — Metastores (Iceberg REST/JDBC catalogs)](https://trino.io/docs/current/object-storage/metastores.html)
- [Trino — S3 file system (MinIO 호환성 명시)](https://trino.io/docs/current/object-storage/file-system-s3.html)
- [MinIO — Documentation](https://min.io/docs/minio/linux/index.html)
- [Apache Parquet — Documentation](https://parquet.apache.org/docs/)

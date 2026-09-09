# 레이크하우스 서버 배치 (MinIO + Iceberg + Trino)

> MinIO(저장) + Iceberg(테이블 포맷) + Trino(SQL 엔진)로 레이크하우스를 구성할 때의 **서버 배치**를 다룹니다. 이 문서는 "어느 서버에 무엇을, 얼마나"에 집중합니다. 3층 구조의 개념과 용도는 [05 레이크하우스와 DB 콜드 데이터](05-lakehouse-and-cold-data.md)를 먼저 참고하세요. 주소·용량은 예시이며 인프라 확정 시 실제 값으로 채웁니다.

## 1. 배치 전에 알아야 할 두 가지

**① Iceberg는 설치하는 게 아니라 Trino에 "설정"하는 것입니다.** Iceberg는 실행되는 서버·프로세스가 아니라 **"MinIO에 쌓인 파일들을 테이블처럼 다루는 규약(테이블 포맷)"** 입니다. 그래서 따로 설치할 "Iceberg 서버"가 없습니다. 대신 **Trino의 Iceberg 커넥터를 설정 파일 하나로 켜서** 사용합니다(설정은 [6장](#6-iceberg-설정-방법-설치가-아니라-trino-커넥터-설정)). 이 커넥터에는 두 대상을 지정합니다 — 데이터를 둘 **저장소(MinIO)**, 테이블 위치·최신 스냅샷을 기록할 **카탈로그**입니다. 카탈로그의 가장 가벼운 형태는 **PostgreSQL 기반 JDBC 카탈로그**입니다. 즉 실제로 띄우는 프로세스는 **MinIO + Postgres + Trino 셋뿐**이고, Iceberg는 그 사이를 잇는 규약입니다.

**② Trino가 이 스택의 주 메모리 소비자입니다.** Trino는 조인·집계·정렬을 **인메모리 파이프라인**으로 수행하고 중간 결과를 힙에 유지합니다. 그래서 메모리가 부족하면 `EXCEEDED_LOCAL_MEMORY_LIMIT`로 쿼리가 실패합니다(디스크 spill은 제한적). 반면 MinIO는 I/O 바운드, Postgres(카탈로그)는 소량 메타데이터만 다뤄 부하가 낮습니다. 따라서 용량 산정의 지배 요인은 **Trino 노드의 힙 크기와 워커 수**입니다.

## 2. 컴포넌트별 역할과 자원 성격

| 컴포넌트 | 역할 | 자원 프로파일 | HA |
| --- | --- | --- | --- |
| MinIO | Parquet 데이터 파일 + Iceberg 메타 파일 저장 (S3 API) | I/O·네트워크 바운드 | [02 배포 형태](02-deployment-topology.md) 참고 |
| **Iceberg** | 파일을 테이블처럼 다루는 **규약(테이블 포맷)** — Trino에 내장 | **런타임 프로세스 없음** (Trino 커넥터 설정으로 사용, [6장](#6-iceberg-설정-방법-설치가-아니라-trino-커넥터-설정)) | 해당 없음 |
| Postgres (카탈로그) | 테이블 목록·스냅샷 포인터 저장 | 저부하·소용량 (2 vCPU/4GB) | 운영 시 백업 필수 |
| Trino coordinator | 쿼리 파싱·플래닝·스케줄링, 결과 취합 | CPU 위주 (힙은 워커보다 작게) | 단일 (대규모 시 전용 노드) |
| Trino worker | 스플릿 단위 데이터 읽기·연산(조인/집계) | **메모리 바운드**(힙) | 데이터·동시성에 비례해 증설 |

## 3. 단계별 서버 배치 (1단계 → 2단계)

MinIO·Trino·카탈로그를 **전부 새로 구축**하는 전제입니다(기존 인프라 없음). 세 컴포넌트는 자원 프로파일이 다릅니다 — MinIO는 I/O 바운드, Trino는 메모리 바운드, 카탈로그는 저부하·고가용 요구입니다. 한 노드에 함께 배치하면 디스크 I/O와 힙 압박이 **자원 경합**을 일으킵니다. 그래서 **처음부터 역할별 3노드로 분리하고, 각 노드를 낮은 스펙으로 시작**합니다.

운영 확대(2단계)에서는 노드 추가(scale-out)보다 **기존 노드의 vCPU·힙 증설(scale-up)을 우선**합니다. Trino 워커는 단일 노드 한계를 넘는 경우에만 추가합니다.

서버 기준으로 두 단계를 보면:

| 서버 | 담는 것 | 1단계 (낮게 시작) | 2단계 (스펙 상향) |
| --- | --- | --- | --- |
| **① MinIO** | MinIO :9000 (S3) + `lakehouse/` 버킷 | 4 vCPU / 8GB / 데이터 SSD | 8 vCPU / 16GB (+ 필요 시 분산 4노드) |
| **② Trino** | Trino coordinator+worker :8080 | 4 vCPU / **16GB** (PoC 한정) | **32~64GB로 상향** (+ 필요 시 워커 노드 추가) |
| **③ 카탈로그** | Postgres (Iceberg JDBC 카탈로그) :5432 | 2 vCPU / 4GB | 4 vCPU / 8GB + **백업**, 필요 시 REST 카탈로그(Nessie) 승격 |
| **서버 수** | | **3대** | 3대 (스펙 상향) ~ 5대 (워커 추가 시) |

- **1단계 — 나누고 낮게**: 3대를 각각 최소 스펙으로 올립니다. Trino 16GB는 **PoC 한정**입니다(공식 운영 권장은 32GB+, 소량 데이터·검증용). MinIO 구축·계정·버킷은 [02 배포 형태](02-deployment-topology.md)·[03 구축 구성안](03-setup-plan.md)을 따릅니다. `lakehouse/` 버킷과 전용 접근 키를 만듭니다.
- **2단계 — 스펙 상향 우선**: 병목이 확인되면 **기존 서버의 vCPU·RAM을 올립니다**(특히 Trino를 공식 운영 하한 32GB+로). 스케일-업으로 감당 안 될 만큼 데이터·동시 쿼리가 커지면 그때 Trino 워커 노드를 추가합니다(scale-out). 공식도 "큰 클러스터는 코디네이터 전용 노드가 최고 성능"이라고 봅니다.
- **카탈로그 백업은 필수**: 카탈로그 서버가 유실되면 데이터 파일이 온전해도 current metadata pointer를 잃어 테이블을 조회할 수 없습니다([5장](#5-카탈로그용-postgres는-왜-필요한가)).

배치도로 보면:

```text
[1단계 — 3대, 낮은 스펙]
서버① MinIO            서버② Trino                 서버③ 카탈로그
MinIO :9000 (S3)       Trino(coordinator+worker)   Postgres :5432
 └ lakehouse/ 버킷 ◀───  :8080  ──(카탈로그 조회)──▶  (Iceberg JDBC 카탈로그)
 4vCPU/8GB              4vCPU/16GB                  2vCPU/4GB

[2단계 — 같은 3대를 스펙 상향 (+ 필요 시 워커 추가)]
서버① MinIO            서버② Trino                 서버③ 카탈로그
 8vCPU/16GB            32~64GB로 상향               4vCPU/8GB + 백업
 (필요 시 분산 4노드)    (+ 워커 노드 추가 가능)        (필요 시 REST 카탈로그)
```

## 4. Trino 스펙 기준 (공식 요구사항)

Trino는 "최소 하드웨어 스펙 표"를 제공하지 않습니다. 필요 RAM이 쿼리 복잡도·데이터 크기·동시 실행 수에 따라 달라지기 때문입니다. 대신 아래 원칙을 명시합니다.

- **Java 25 필수** (최소 25.0.1, 64-bit). Java 8/11/17/21/24 및 26+는 미지원입니다. 사내 표준 JDK가 낮으면 Trino 노드에만 JDK 25를 따로 설치하거나 **공식 Docker 이미지**(JDK 내장)를 씁니다.
- **JVM 힙(`-Xmx`) = 노드 RAM의 70~85%.** 예: 32GB 노드 → `-Xmx24G`, 64GB → `-Xmx54G`. 나머지는 OS·off-heap용입니다.
- **운영은 32GB 이상 권장.** 16GB는 공식 `jvm.config` 예시값일 뿐 권장 최소 사양이 아닙니다(학습·극소량 전용). 위 표에서 1단계는 16GB로 시작하되 2단계에 32GB+로 올리는 이유입니다.
- **swap 비활성화.** OS 레벨에서 swap을 끄는 것을 공식 권장합니다. VM 생성 시 함께 요청하세요.
- RAM은 클수록 더 큰 조인·집계가 가능합니다. "최소"보다 "얼마나 줄 수 있나"가 성능을 좌우합니다.

## 5. 카탈로그용 Postgres는 왜 필요한가

데이터가 전부 MinIO에 있는데 왜 카탈로그 DB(Postgres)가 별도로 필요할까요. 이유는 Iceberg의 메타데이터 구조와 커밋 프로토콜에 있습니다.

**Iceberg의 메타데이터 계층**: Iceberg 테이블은 MinIO에 데이터 파일(Parquet)과 **메타데이터 트리**로 저장됩니다(metadata.json → manifest list → manifest → 데이터 파일). 테이블에 write가 일어날 때마다 **새 metadata.json(스냅샷)** 이 생성됩니다. 이전 버전은 지워지지 않고 남습니다. 이 이력이 스냅샷 조회·타임트래블의 근거입니다. 그래서 "**현재 유효한** metadata.json이 어느 것인가"를 가리키는 **단일 포인터**가 필요합니다. 이 **current metadata pointer**를 관리하는 것이 카탈로그입니다.

**포인터를 오브젝트 스토리지가 아니라 DB에 두는 이유**: 커밋은 이 포인터를 *이전 metadata → 새 metadata* 로 교체하는 연산입니다. 동시에 write가 일어나면 이 교체가 **원자적 compare-and-swap(CAS)** 이어야 정확합니다. 경쟁에서 진 커밋은 충돌로 실패하고 재시도합니다(낙관적 동시성 제어). 오브젝트 스토리지는 이런 조건부 원자적 갱신을 표준적으로 보장하지 않습니다(S3 조건부 write는 제약이 있음). 반면 **RDBMS 트랜잭션은 CAS를 기본 제공**합니다. 그래서 대용량 데이터·메타 파일은 MinIO에 두고, 포인터 갱신(커밋)만 트랜잭션이 보장되는 저장소(Postgres 등)에 위임합니다.

정리하면 역할이 이렇게 나뉩니다:

| 저장 대상 | 위치 | 근거 |
| --- | --- | --- |
| 데이터 파일(Parquet) + 메타데이터 트리 | **MinIO** | 대용량·다수 객체 → 오브젝트 스토리지의 비용·확장성 |
| current metadata pointer + 테이블/네임스페이스 목록 | **Postgres(카탈로그)** | 커밋의 원자적 CAS에 **트랜잭션** 필요 |

- 카탈로그 DB는 **포인터와 네임스페이스·테이블 목록 등 메타데이터만** 보관합니다. 그래서 데이터량이 작아 2 vCPU/4GB로 충분합니다. 실제 데이터·메타 파일은 전부 MinIO에 있습니다.
- 카탈로그 DB **유실 시** MinIO의 데이터·메타 파일이 온전해도 current metadata pointer를 잃어 테이블을 조회할 수 없습니다. 수동 복구는 metadata 경로 재등록이 필요해 비용이 큽니다. 문서 곳곳에서 **카탈로그 백업 필수**를 강조하는 이유입니다.
- Postgres는 카탈로그를 담는 **한 가지 선택지**입니다. 이 포인터 관리 역할을 통틀어 "카탈로그"라 부르며, 구현 방식은 JDBC(Postgres 직접)와 REST(Nessie 등)로 나뉩니다([1장 ①](#1-배치-전에-알아야-할-두-가지)). 1단계는 가장 단순한 Postgres JDBC로 시작합니다.

## 6. Iceberg 설정 방법 (설치가 아니라 Trino 커넥터 설정)

Iceberg는 설치할 프로세스가 없습니다. 따라서 "Iceberg를 구성한다"는 것은 곧 **Trino에 Iceberg 커넥터 설정 파일 한 개를 놓는 것**입니다. Trino가 Iceberg 라이브러리를 이미 내장하고 있어, 운영자 입장에서는 이 파일이 사실상 "Iceberg 설치"에 해당합니다.

Trino의 `etc/catalog/iceberg.properties`입니다. 파일명이 곧 카탈로그 이름이 되어 SQL에서 `iceberg.스키마.테이블`로 참조합니다.

```properties
connector.name=iceberg
# 테이블 위치를 기록할 카탈로그 — 여기서는 서버③의 Postgres(JDBC)
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=lakehouse
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://서버③:5432/iceberg
iceberg.jdbc-catalog.connection-user=iceberg
iceberg.jdbc-catalog.connection-password=${ICEBERG_CATALOG_PW}
iceberg.jdbc-catalog.default-warehouse-dir=s3://lakehouse/warehouse

# 데이터를 둘 저장소 — 서버①의 MinIO (S3 호환)
fs.native-s3.enabled=true
s3.endpoint=https://서버①:9000
s3.region=us-east-1            # MinIO 는 값 무관, 형식상 필요
s3.path-style-access=true      # MinIO 는 path-style 필요
s3.aws-access-key=${MINIO_ACCESS_KEY}
s3.aws-secret-key=${MINIO_SECRET_KEY}
```

- 이 파일 하나가 **"저장소(MinIO) + 카탈로그(Postgres)를 Iceberg로 묶어라"** 라는 지시입니다. 별도 Iceberg 데몬 기동은 없습니다.
- 비밀값(`${...}`)은 환경변수/Secret으로 주입하고 저장소에 커밋하지 않습니다. 계정·키 발급은 MinIO 쪽 [03 구축 구성안](03-setup-plan.md) 4장을 따릅니다.
- 속성명은 Trino 버전에 따라 다를 수 있으니 실제 배포 버전 문서로 확인하세요(최신 Trino는 위 `fs.native-s3.*` 네이티브 파일시스템 방식).
- 2단계에서 REST 카탈로그(Nessie)로 승격하면 `iceberg.catalog.type=rest` + `iceberg.rest-catalog.uri=...`로 바뀝니다. 커넥터 설정만 교체될 뿐 MinIO·데이터는 그대로입니다.

설정이 끝나면 표준 SQL로 스키마·테이블을 만들고 바로 씁니다. 이 시점부터 MinIO에 Parquet·메타 파일이 생기고, Postgres에 테이블 포인터가 기록됩니다.

```sql
CREATE SCHEMA iceberg.archive;
CREATE TABLE iceberg.archive.orders (
    order_id   BIGINT,
    amount     BIGINT,
    created_at TIMESTAMP(6)
) WITH (format = 'PARQUET');

INSERT INTO iceberg.archive.orders VALUES (1001, 55000, TIMESTAMP '2026-09-09 10:00:00');
SELECT * FROM iceberg.archive.orders;
```

## 7. 적재 경로 (데이터를 Iceberg에 넣기)

배치와 별개로 정합니다:

- **배치 아카이빙** — Trino가 사내 RDB를 JDBC로 읽어 `INSERT INTO iceberg... SELECT FROM mysql...` ([05 레이크하우스](05-lakehouse-and-cold-data.md) 2장). 추가 서버 불요.
- **스트리밍** — Kafka → Kafka Connect(Kafka 로드맵에 이미 있음) + Iceberg Sink → Iceberg 테이블. 새 인프라 종류가 늘지 않습니다. 단, 스트리밍 적재는 작은 파일이 쌓이므로 **주기적 컴팩션** 운영이 따라옵니다.

## 8. 모니터링·보안 (기존 재사용)

- **모니터링**: Trino는 JMX/Prometheus 메트릭을 노출 — Kafka 때 구축 예정인 Prometheus/Grafana에 스크레이프 잡만 추가.
- **인증서**: Trino·MinIO의 TLS는 Kafka 때 만든 사내 CA를 재사용.
- **방화벽**: 사용자·BI 도구 → Trino :8080, Trino → MinIO :9000, Trino → Postgres :5432. Postgres·MinIO 내부 포트는 분석 노드 대역만 개방.

## 9. 도입 순서와 현실 체크

전부 신규 구축이므로 권장 순서: **① MinIO 신규 구축([02](02-deployment-topology.md)·[03](03-setup-plan.md)) → ② Trino + 카탈로그 추가(1단계) → ③ 배치 아카이빙으로 첫 데이터 적재·검증 → ④ 실측 후 워커 증설(2단계).** 스트리밍 적재가 필요하면 그 시점에 Kafka Connect를 얹습니다.

- 서버가 빠듯하면 처음부터 2단계(워커 여러 대)로 가지 마세요. 1단계 최소 구성으로 파일럿을 돌려 **실제 쿼리가 메모리를 얼마나 쓰는지 측정**한 뒤 확장을 결정합니다. 공식이 정확한 스펙 공식을 주지 않는 것도 같은 이유입니다.
- MinIO를 S3 표준으로 새로 깔아두면 두 용도를 겸합니다 — 이 레이크하우스의 저장층이자 사내 **파일 저장소**([01](01-what-is-minio.md))입니다. 한 번 구축해 둘 다 쓰므로 이중 투자가 없습니다.
- 분석 용도가 붙으면 MinIO 용량 성장 속도가 파일 저장소와 차원이 달라집니다(수백 GB~TB). 그 시점에 디스크 증설 계획을 다시 잡습니다.

## 관련 문서

- [05 레이크하우스와 DB 콜드 데이터](05-lakehouse-and-cold-data.md) — 3층 구조 개념, RDB 아카이빙 패턴
- [02 배포 형태와 서버 스펙](02-deployment-topology.md) — MinIO 배포 형태·용량 산정
- [03 구축 구성안](03-setup-plan.md) — MinIO Compose·계정·버킷·백업

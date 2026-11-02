# SeaweedFS

S3 호환 오브젝트 스토리지 SeaweedFS를 파일 저장소와 레이크하우스 저장 계층으로 쓰기 위한 설치·구성 문서입니다. Apache-2.0 오픈소스이며, 유료 Enterprise와 같은 바이너리를 쓰고 라이선스 키로만 기능이 추가됩니다. 운영 사용에 별도 라이선스가 필요하지 않습니다.

## 왜 SeaweedFS인가

MinIO 커뮤니티 버전은 2025-12 유지보수 모드를 거쳐 2026-04-25 GitHub 저장소가 아카이브되었습니다. 바이너리와 보안 패치가 더 제공되지 않습니다. 후속 제품 AIStor는 무료판이 단일 노드 한정이라 3대 분산 구성을 할 수 없습니다. 기존 [MinIO 문서](../minio/README.md)의 역할 분담과 계정·버킷 원칙은 그대로 가져오고, 저장소 제품만 SeaweedFS로 바꿉니다.

| 항목 | SeaweedFS |
| --- | --- |
| 라이선스 | Apache-2.0. 운영 사용 제한 없음 |
| 구조 | Master(볼륨 위치 관리) · Volume(실제 파일 보관) · Filer(디렉터리·메타데이터) · S3 Gateway(S3 API) |
| 단일 노드 | `weed mini` 한 프로세스에 네 구성요소 + Admin UI + 유지보수 워커 포함 |
| S3 호환 | 멀티파트 업로드, ListObjectsV2, DeleteObjects, 버킷 버저닝 지원 (단일 노드 설치 문서에서 직접 검증) |
| 계정·권한 | 사용자별 액세스 키 + 버킷 한정 정책 (readonly / readwrite / admin) |
| 부가 기능 | Iceberg REST Catalog, WebDAV, Prometheus 메트릭 내장 |

## 문서

- [01 단일 노드 설치 (weed mini)](concepts/01-install-single-node.md) — Docker Compose로 한 대에 띄우고, 버킷 한정 계정을 만들고, AWS CLI로 S3 호환을 검증하는 순서

실행 가능한 예제:

- [단일 노드 Compose](examples/single-node/README.md) — 01 문서가 쓰는 `docker-compose.yml`과 `.env.example`

## 운영 구성 요약 (02 문서 예정)

역할은 Master·Volume·Filer·S3 Gateway 4개지만, 설치를 4번 하는 것은 아닙니다. 바이너리는 `weed` 하나이고 하위 명령으로 역할을 골라 띄웁니다. 운영 3대에서는 서버마다 다음 3개 프로세스를 올립니다.

| 프로세스 | 포트 | 역할 |
| --- | --- | --- |
| `weed master` | 9333 | 3대가 Raft로 리더 선출. 2대로는 합의가 안 되므로 반드시 3대 |
| `weed volume` | 8080 | 실제 파일 보관. 복제 `001` 이상으로 서버 1대 장애 허용 |
| `weed filer -s3` | 8888, S3 8333 | 메타데이터 + S3 API. 공식 문서가 S3를 Filer와 함께 띄우라고 안내 |

Filer가 3개면 메타데이터 저장소를 공유해야 합니다. 기본 leveldb는 Filer 1개용이고, 운영은 Postgres를 씁니다. 레이크하우스 카탈로그용 Postgres에 DB 하나를 더 두면 서버가 늘지 않습니다. 이 Postgres가 저장소의 단일 장애점이므로 백업과 함께 둡니다. 앱과 Trino는 L4로 3대의 8333 중 하나에 붙습니다.

weed mini와 운영 구성의 항목별 차이는 [01 문서 10장](concepts/01-install-single-node.md)에 있습니다.

## 작성 예정

- 02 3대 분산 구성 — 위 요약의 실제 명령, `filer.toml`(Postgres), `s3.json`, L4 설정
- 03 계정·버킷 체계와 백업 — 서비스별 계정, 버킷 버저닝, `weed filer.backup`
- 04 레이크하우스 연결 — Trino·Kafka Connect·Iceberg Catalog에서 S3 엔드포인트 설정. 카탈로그 DB는 Postgres 기준(Polaris·Lakekeeper는 Postgres만 지원, Nessie는 MariaDB도 지원)

## 상태

2026-11-02 기준 SeaweedFS 4.50으로 로컬 Docker에서 단일 노드 설치와 S3 검증을 마쳤습니다. 실서버 배치는 미정입니다.

## 참고

- [SeaweedFS GitHub](https://github.com/seaweedfs/seaweedfs)
- [Quick Start with weed mini](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini)
- [Amazon S3 API 호환 범위](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)
- [S3 Credentials](https://github.com/seaweedfs/seaweedfs/wiki/S3-Credentials)
- [Open Source vs Enterprise](https://seaweedfs.com/docs/open-source-vs-enterprise/)

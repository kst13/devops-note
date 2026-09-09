# MinIO

S3 호환 오브젝트 스토리지 MinIO를 사내 파일 저장소로 도입하기 위한 검토·설계 문서입니다. 배포 형태 선택부터 구성안, 기존 파일 저장 애플리케이션의 이전 전략, 그리고 데이터 레이크하우스(Iceberg + Trino)로의 확장까지 다룹니다.

## 문서

- [01 MinIO란 무엇이고 왜 필요한가](concepts/01-what-is-minio.md) — 오브젝트 스토리지와 S3 API, 현재 파일 저장 방식의 문제, 도입 검토 항목
- [02 배포 형태와 서버 스펙](concepts/02-deployment-topology.md) — SNSD/SNMD/MNMD 선택 기준, 물리/VM 판단, 스펙·용량 산정
- [03 구축 구성안](concepts/03-setup-plan.md) — Compose 구성, TLS, 계정·버킷 체계, 백업, 모니터링, 확장 경로
- [04 기존 파일 애플리케이션 이전](concepts/04-file-app-migration.md) — 무중단 병행 전환 절차
- [05 레이크하우스와 DB 콜드 데이터](concepts/05-lakehouse-and-cold-data.md) — Iceberg·Trino 연계, RDB 아카이빙 패턴
- [06 레이크하우스 서버 배치](concepts/06-lakehouse-deployment-layout.md) — MinIO+Iceberg+Trino 단계별 배치, 노드 스펙, Trino 공식 요구사항

## 상태

도입 검토 단계입니다. 인프라(VM·스토리지) 확정 전이며, 구성안의 주소·용량 값은 예시입니다.

# 구축 구성안 (검토 중)

> VM + SNSD(하부 스토리지 이중화 전제) 기준의 구성안입니다. 물리 서버 SNMD로 갈 경우의 차이는 각 절에 표시했습니다. 주소·용량은 예시이며 인프라 확정 시 실제 값으로 채웁니다.

## 1. 서버·디스크

- **전용 VM 1대** — Kafka 브로커 서버에 얹지 않습니다(디스크 I/O·페이지 캐시 경쟁). 파일 저장소는 데이터 경로이므로 "죽어도 무영향"이어야 하는 관제 서버 원칙과도 충돌합니다.
- 스펙: 4~8vCPU / 16GB / 데이터 가상디스크 1TB(XFS, `/mnt/minio/data`) — 산정 근거는 [02 배포 형태](02-deployment-topology.md) 4·5장.
- SNMD(물리)라면: 동일 용량 SSD 4장을 각각 XFS로 `/mnt/minio/data1~4`에 마운트, RAID 미사용.

## 2. Compose

```yaml
services:
  minio:
    image: minio/minio:RELEASE.XXXX   # 설치 시점 최신 안정 태그로 고정 (latest 금지)
    container_name: minio
    restart: unless-stopped
    command: server /data --console-address ":9001"        # SNMD 는 /data{1...4}
    ports:
      - "9000:9000"   # S3 API (앱)
      - "9001:9001"   # 웹 콘솔 (관리자)
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}           # 관리 전용 — 앱 배포 금지
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}   # .env, 커밋 금지
    volumes:
      - /mnt/minio/data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
```

## 3. TLS

- Kafka 구축 때 만든 **사설 CA를 재사용**해 이 서버용 인증서를 발급합니다(SAN에 DNS 이름·IP 포함). Java 앱들이 이미 그 CA를 신뢰하고 있어 truststore 추가 배포가 없습니다.
- 인증서를 MinIO의 `certs/` 경로에 두면 자동으로 HTTPS가 켜집니다. 대안: 앞단 nginx에서 TLS 종단.

## 4. 계정·버킷 체계 (Kafka와 같은 거버넌스 모델)

- 루트 계정은 관리용으로만 — 앱에는 **서비스별 access key/secret key** + **버킷 단위 정책**으로 자기 버킷만 접근.
- 버킷 명명: `<서비스>-<용도>` (예: `notification-attachments`). 용도별 분리 — 파일 버킷과 레이크하우스 버킷은 접근 주체가 다릅니다([05](05-lakehouse-and-cold-data.md) 3장).

```bash
mc alias set our-minio https://minio.internal:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

mc mb our-minio/notification-attachments
mc version enable our-minio/notification-attachments      # 버저닝 — 삭제 실수 1차 방어

mc admin user add our-minio notification-service '발급한시크릿키'
mc admin policy create our-minio notification-rw policy-notification.json   # 해당 버킷 한정 정책
mc admin policy attach our-minio notification-rw --user notification-service
```

- 발급물(엔드포인트·키·버킷명)과 신청 절차는 Kafka 온보딩·발급 런북의 틀을 재사용합니다.
- 버저닝과 함께 **수명주기 규칙**(예: 옛 버전 30일 후 삭제)을 걸어 버저닝 오버헤드를 통제합니다.

## 5. 백업

- 다른 장비(**다른 스토리지 위**의 서버·NAS — 같은 데이터스토어의 다른 VM은 무의미)로 크론 미러링:

```bash
# --remove 는 쓰지 않는다 — 원본의 삭제가 백업까지 전파되면 백업의 의미가 없다
0 * * * * mc mirror --overwrite our-minio/ backup-target/minio-backup/
```

- 백업 주기 = 허용 유실 시간(RPO). 분기 1회 **복구 리허설**(백업에서 실제로 읽어보기)을 포함합니다 — 검증 안 된 백업은 없는 백업과 같습니다.

## 6. 모니터링·방화벽

- MinIO는 Prometheus 메트릭 내장 — 기존 Prometheus에 스크레이프 잡 추가 + 공식 Grafana 대시보드 임포트.
- 방화벽: 앱 서버 대역 → 9000, 관리자 → 9001, Prometheus 서버 → 9000.

## 7. 지금 해두면 확장이 쉬워지는 것

1. 앱에는 IP가 아니라 **DNS 이름**(`minio.내부도메인`)으로 엔드포인트 배포.
2. 버킷 **버저닝을 처음부터** 활성화.

## 8. 확장 경로

- **용량 부족 (같은 서버)** — VM-SNSD는 가상디스크 확장 + `xfs_growfs`(온라인). SNMD는 디스크 4개 한 세트(풀)를 추가하고 기동 인자에 풀을 덧붙여 재기동(`server /data{1...4} /data{5...8}`). **기존 풀 표기는 절대 변경 금지.**
- **HA 필요 (분산 전환)** — 단일 노드 → MNMD "전환"은 지원되지 않습니다. 정석: 서버 4대로 새 분산 클러스터 구축 → `mc mirror`(버저닝 시 사이트 복제)로 이관 → **DNS 전환**(7장 1번이 이때를 위한 포석 — 앱 설정 무변경) → 구 서버는 백업 대상으로 전환.

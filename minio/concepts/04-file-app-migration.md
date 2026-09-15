# 기존 파일 애플리케이션 이전

> 현재 파일은 별도의 파일 저장 애플리케이션이 같은 서버에 마운트된 저장소에 직접 쓰는 구조입니다. 이 앱의 저장 백엔드를 MinIO(S3)로 교체하는 것이 이전의 본체이며, 완료 시 기존 저장소를 회수합니다.

## 1. 유리한 조건

파일 I/O가 여러 서비스에 흩어져 있지 않고 **이 앱 한 곳에 모여 있습니다.** 이 앱이 다른 서비스들에게 업로드/다운로드 API를 제공하는 구조라면, 다른 서비스들은 아무 변경 없이 이 앱의 저장 계층만 `java.io.File` → S3 SDK로 바꾸면 됩니다.

## 2. 선택지

| 방안 | 방법 | 판단 |
| --- | --- | --- |
| **① 앱을 S3로 전환** | 파일 읽기/쓰기 코드를 S3 SDK로 교체 | **권장** — 서버-파일 결합이 진짜로 풀림 |
| ② 파일시스템처럼 마운트 | s3fs·rclone mount로 버킷을 디렉터리로 위장, 앱 무수정 | 코드를 못 고치는 레거시용 임시방편 — 성능 저하, 파일 잠금·부분쓰기 동작 차이 주의 |
| ③ 현상 유지 | 신규 서비스만 MinIO 사용 | 이전 효과(디스크 회수·표준화)가 없음 |

## 3. 무중단 병행 전환 절차 (① 기준)

```text
1단계  MinIO 구축 + 버킷 생성 + 버저닝 켜기, 앱에 키 발급
2단계  앱 수정: 쓰기는 MinIO 로(ETag 검증·저장), 읽기는 "MinIO 먼저 → 없으면 기존 경로" (이중 조회)
3단계  기존 파일 백필: mc mirror /mnt/기존저장소 our-minio/버킷   (운영 중 백그라운드)
4단계  검증: 파일 수·용량 대조, 전수 해시 대조
5단계  이중 조회 제거(읽기도 MinIO 만) → 관찰 기간
6단계  기존 마운트 저장소 회수 → 인프라팀에 반납 보고
```

- **2단계의 이중 조회**가 핵심입니다 — 백필이 끝나기 전에도 서비스가 정상 동작하므로 빅뱅 전환(장시간 중단 + 일괄 복사)을 피할 수 있습니다.
- `mc mirror`는 로컬 디렉터리 → 버킷 복사를 지원하고 변경분만 다시 돌릴 수 있습니다. 1차 복사 후 전환 직전에 한 번 더 돌려 따라잡는 식으로 진행합니다.
- **오브젝트 키 설계**: 기존 디렉터리 구조를 그대로 키로 가져가면(`2026/09/uuid.pdf`) 백필과 이중 조회 구현이 단순해집니다.

현재 파일 서비스의 두 문제인 "저장 확인 불가"와 "삭제 복구 불가"([01](01-what-is-minio.md) 2장)는 이 절차의 1·2·4단계에서 해결됩니다. 아래에 단계별로 풀어 씁니다.

### 1단계 — 버저닝은 버킷을 만들자마자 켠다

버저닝은 삭제와 덮어쓰기를 "새 버전 추가"로 기록하는 기능입니다. 켜기 전에 넣은 객체는 보호되지 않습니다. 그래서 백필(3단계)보다 먼저 켜야 합니다.

```bash
mc mb --ignore-existing our-minio/files
mc version enable our-minio/files
mc version info our-minio/files          # Enabled 확인
```

오래된 버전이 무한히 쌓이지 않게 수명주기 규칙을 함께 겁니다. 실수 삭제를 되돌릴 창은 확보하면서 용량은 제한하는 것이 목적입니다.

```bash
# 이전 버전은 90일 뒤 삭제, 삭제 마커만 남은 객체는 정리
mc ilm rule add our-minio/files --noncurrent-expire-days 90 --expire-delete-marker
```

복구는 한 줄입니다. 규정상 지우면 안 되는 파일은 Object Lock으로 보관 기간을 강제합니다. Object Lock은 버킷 생성 시에만 켤 수 있으므로 필요하면 `mc mb --with-lock`으로 처음부터 만듭니다.

```bash
mc ls --versions our-minio/files/2026/09/report.pdf     # 버전 목록
mc undo our-minio/files/2026/09/report.pdf              # 마지막 삭제·덮어쓰기 되돌리기
```

### 2단계 — 업로드 응답의 ETag를 검증하고 저장한다

ETag는 MinIO가 저장한 객체의 해시입니다. 단일 PUT이면 MD5와 같습니다. 앱은 보내기 전에 계산한 해시와 응답 ETag를 비교해 "온전히 저장됐다"를 그 자리에서 확인합니다. AWS SDK v2는 `ChecksumAlgorithm`을 지정하면 전송 중 검증까지 자동으로 합니다.

```java
byte[] md5 = MessageDigest.getInstance("MD5").digest(bytes);
String expected = HexFormat.of().formatHex(md5);

PutObjectResponse res = s3.putObject(
        b -> b.bucket("files").key(key)
              .contentMD5(Base64.getEncoder().encodeToString(md5))   // 서버가 대조, 불일치면 400
              .checksumAlgorithm(ChecksumAlgorithm.SHA256),           // 전송 무결성
        RequestBody.fromBytes(bytes));

String etag = res.eTag().replace("\"", "");
if (!etag.equals(expected)) {
    throw new IllegalStateException("저장 검증 실패: " + key);       // 재시도 대상
}
fileMetaRepository.save(new FileMeta(key, bytes.length, etag, res.versionId()));
```

- `contentMD5`를 보내면 MinIO가 수신 데이터와 대조해 불일치 시 저장을 거부합니다. 앱은 응답 ETag까지 한 번 더 확인합니다.
- **ETag와 versionId를 DB의 파일 메타데이터에 함께 저장**합니다. 나중에 "이 파일이 그 파일이 맞나"를 대조할 근거이고, 특정 버전을 지정해 내려받을 수도 있습니다.
- 5MB 이상을 멀티파트로 올리면 ETag가 MD5가 아닙니다(`<해시>-<파트수>` 형태). 이 경우 `ChecksumAlgorithm`의 SHA256 체크섬을 응답에서 읽어 저장하거나, 업로드 후 `HEAD`로 크기와 체크섬을 확인합니다.
- 존재·상태 확인은 파일시스템 `ls`가 아니라 `HEAD` 한 번입니다. 운영 스크립트에서는 `mc stat`입니다.

```bash
mc stat our-minio/files/2026/09/report.pdf     # Size · ETag · VersionID · 저장 시각
```

### 4단계 — 백필은 전수 해시 대조로 검증한다

건수와 용량만 맞추면 깨진 파일이 그대로 옮겨진 것을 놓칩니다. 기존 저장소의 파일이 이미 일부 손상됐을 수 있으므로, 이 단계가 그것을 드러내는 마지막 기회입니다.

```bash
# ① 건수·용량
find /mnt/files -type f | wc -l;  du -sb /mnt/files
mc ls -r --summarize our-minio/files

# ② 전수 해시 대조 — 원본 MD5 와 MinIO ETag 비교 (단일 PUT 객체 기준)
find /mnt/files -type f -print0 | xargs -0 md5sum | sed 's|/mnt/files/||' | sort -k2 > /tmp/src.md5
mc ls -r --json our-minio/files | jq -r '"\(.etag | ltrimstr("\"") | rtrimstr("\""))  \(.key)"' | sort -k2 > /tmp/dst.md5
diff /tmp/src.md5 /tmp/dst.md5          # 출력이 없어야 통과

# ③ 남은 차이는 mirror 가 다시 잡는다 (변경분만)
mc mirror --overwrite /mnt/files our-minio/files
```

- `mc mirror`가 멀티파트로 올린 큰 파일은 ETag가 MD5와 다릅니다. ②에서 불일치로 나온 파일 중 크기가 큰 것은 `mc cat our-minio/files/<key> | md5sum`으로 내려받아 다시 비교합니다.
- ②에서 원본 쪽 해시가 DB 메타데이터와도 다르면 **기존 저장소에서 이미 깨진 파일**입니다. 목록을 남기고 업무 담당자와 처리 방법(재업로드 요청, 폐기)을 정합니다.
- 통과한 뒤에만 5단계로 갑니다. 원본 회수(6단계)는 관찰 기간이 끝난 뒤입니다.

### 이전 후 달라지는 운영

| 상황 | 이전 (파일시스템) | 이후 (MinIO) |
| --- | --- | --- |
| 저장 확인 | 열어 봐야 안다 | 업로드 시 ETag 대조, 이후 `mc stat` |
| 실수 삭제 | 백업 시점으로만 | `mc undo`로 파일 단위 즉시 복구 |
| 누가 지웠나 | 기록 없음 | 감사 로그(`mc admin trace` 또는 audit webhook) |
| 깨진 파일 | 발견 시점에 이미 늦음 | 읽기 시 해시 대조, 분산 구성이면 자동 복원 |

## 4. 수정 범위와 전환 방식

코드 분석 기준으로 손대는 곳은 다섯 군데입니다.

| 대상 | 지금 | 바꾸는 것 |
| --- | --- | --- |
| `FileServiceImpl` | 로컬 경로에 저장·조회 | 스토리지 인터페이스 호출로 교체 |
| `TransferHandler` | temp → storage `Files.move` | 서버 사이드 `CopyObject` + 원본 삭제 |
| `FileController` 다운로드·리사이징 | `ByteArrayResource` 전체 적재, AES 복호화 임시 파일 | 임시 URL 발급(SSE 채택 시) 또는 스트리밍 중계(앱 암호화 유지 시) |
| 메일 첨부 조회 | `walkFileTree` 탐색 | DB의 버킷·키 컬럼으로 직접 접근 |
| `FileScheduler` | 30일 임시 파일 삭제 | 제거. `temp` 버킷 수명주기 규칙으로 대체 |

전환은 **스토리지 인터페이스를 하나 두고 로컬 구현과 MinIO 구현을 나란히** 둡니다. 설정으로 구현체를 고르면 3장의 이중 조회(MinIO 먼저 → 없으면 로컬)를 인터페이스 안에서 처리할 수 있고, 문제가 생기면 로컬 구현으로 되돌립니다. DB의 `FILE_PATH` 컬럼은 의미가 로컬 경로에서 버킷·키로 바뀌므로, 마이그레이션 배치가 기존 행을 채우고 새 저장은 키를 기록합니다.

암호화 방식은 전환 전에 정합니다([01](01-what-is-minio.md) 2장). SSE로 가면 마이그레이션 배치가 디스크의 암호화 파일을 앱 키로 복호화해 MinIO에 올리고, MinIO가 다시 서버 사이드로 암호화합니다. 앱 암호화를 유지하면 파일을 그대로 올리되 다운로드 경로는 임시 URL 대신 앱 중계로 남깁니다.

## 5. 확인해야 할 것

- 앱 수정 가능 여부(사내 개발/소스 보유 여부) — 불가하면 ②로 우회하거나 신규분만 먼저(③) 점진 전환.
- **파일을 읽는 경로 전수 조사** — 다른 서비스가 이 앱의 API를 거치는지, 웹서버(nginx 등)가 마운트 경로를 직접 서빙하는 곳은 없는지. 직접 서빙이 있다면 그 경로도 전환 대상입니다(MinIO presigned URL 또는 프록시 경유로 대체).

## 6. 인프라 협의 관점

이 이전은 "신규 저장 공간 요청"이 아니라 **기존 사용분(예: 2TB 중 400GB)의 표준 저장소 이관 + 완료 시 기존 저장소 반납**입니다. 요청 리소스는 VM 1대뿐이고, 회수 계획을 명시하면 전체 스토리지 관점에서는 정리 사업이 됩니다. 흩어진 서버별 파일 디스크 여유 공간이 중앙으로 통합되는 효과도 있습니다.

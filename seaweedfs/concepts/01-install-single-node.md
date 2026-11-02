# 단일 노드 설치 (weed mini)

> SeaweedFS를 한 대에 띄우는 가장 단순한 방법입니다. 로컬 PC의 PoC와 개발 VM의 공용 저장소에 쓰고, 운영 3대 구성은 02 문서에서 다룹니다. 이 문서의 모든 명령과 출력은 SeaweedFS 4.50, Docker 28에서 실제로 실행해 확인했습니다.

## 1. 무엇을 설치하는가

SeaweedFS는 네 가지 프로세스로 이루어집니다. `weed mini`는 이 넷을 한 프로세스에 묶고 Admin UI와 유지보수 워커까지 함께 띄웁니다.

| 구성요소 | 역할 | 포트 | 외부 공개 |
| --- | --- | --- | --- |
| Master | 어느 Volume에 어떤 파일이 있는지 관리. 쓰기 위치 배정 | 9333 | 사내망 한정 (상태 확인용) |
| Volume | 실제 파일 보관. 여러 파일을 큰 볼륨 파일 하나에 이어 붙여 저장 | 9340 | 공개 안 함 |
| Filer | 디렉터리 구조와 파일 메타데이터. S3의 버킷·키가 여기 기록됨 | 8888 | 사내망 한정 (파일 트리 확인용) |
| S3 Gateway | S3 API. 앱·Trino·Kafka Connect가 쓰는 유일한 입구 | 8333 | 공개 |
| Admin UI | 계정·정책·볼륨 유지보수 화면 | 12535 | 사내망 한정 (비밀번호) |
| Iceberg REST Catalog | 내장 Iceberg 카탈로그 | 8181 | 이 문서에서는 쓰지 않음 |
| WebDAV | 파일 마운트용 | 7333 | 쓰지 않음 |

`weed mini`의 한계는 Master가 하나라는 점입니다. 서버가 죽으면 저장소 전체가 멈춥니다. 복제도 없습니다. 그래서 로컬·개발용이고, 운영은 Master 3대 구성으로 올립니다.

## 2. 사전 조건

- Docker Engine과 Compose v2. `docker compose version`이 `v2.x`를 출력해야 합니다.
- 비어 있는 포트 8333, 8888, 9333, 12535. 아래 명령이 아무것도 출력하지 않으면 됩니다.

```bash
for p in 8333 8888 9333 12535; do lsof -nP -iTCP:$p -sTCP:LISTEN; done
```

- 데이터를 둘 디스크. 로컬은 홈 디렉터리면 충분합니다. 개발 VM이면 별도 디스크를 `/data/seaweedfs`로 마운트하고 아래 `./data`를 그 경로로 바꿉니다.

## 3. 작업 디렉터리와 파일

먼저 어디에 무엇이 생기는지 알아 둡니다. 모든 작업은 한 디렉터리 안에서 합니다.

```text
~/seaweedfs/                      ← 작업 디렉터리 (이름은 자유)
├── docker-compose.yml            ← 이 문서의 3-1 내용. 저장소의 examples/single-node 에서 복사
├── .env                          ← 관리자 키·Admin UI 비밀번호. 직접 생성, 커밋 금지
└── data/                         ← 컨테이너가 만든다. 메타데이터 + 볼륨 파일. 백업 단위
    ├── m9333/                    ← Master 메타데이터 (볼륨 id 배정 상태)
    ├── filerldb2/                ← Filer 메타데이터 (버킷·키·계정·정책). 잃으면 파일을 못 찾는다
    ├── 1.dat, 1.idx              ← Volume 파일. .dat 가 실제 데이터, .idx 가 위치 색인
    ├── lakehouse_2.dat, .idx     ← 버킷별로 컬렉션이 나뉘어 볼륨 파일 접두어가 붙는다
    ├── admin/, worker/           ← Admin UI 설정, 유지보수 작업 로그
    ├── mini.options              ← 기동 시 적용된 옵션 기록
    └── .mini_sse_kek             ← 서버측 암호화 키. 함께 백업해야 암호화 객체를 읽는다
```

`data/`는 디렉터리 하나가 저장소 전체입니다. Filer 메타데이터와 Volume 파일을 따로 백업하면 복구가 안 됩니다. 항상 통째로 다룹니다.

### 3-1. docker-compose.yml

저장소의 [examples/single-node/docker-compose.yml](../examples/single-node/docker-compose.yml)을 복사합니다. 내용은 다음과 같고, 옵션마다 이유를 적었습니다.

```yaml
name: seaweedfs

services:
  seaweedfs:
    image: chrislusf/seaweedfs:4.50          # 태그 고정. latest 는 기본값이 바뀔 수 있다
    container_name: seaweedfs
    restart: unless-stopped
    # 이미지의 ENTRYPOINT 가 weed 이므로 하위 명령부터 적는다
    command: >
      mini -dir=/data
      -master.defaultReplication=000
      -master.telemetry=false
      -admin.password=${SEAWEEDFS_ADMIN_PASSWORD}
      -bucket=lakehouse
    environment:
      AWS_ACCESS_KEY_ID: ${SEAWEEDFS_ROOT_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${SEAWEEDFS_ROOT_SECRET_KEY}
    ports:
      - "8333:8333"     # S3 API
      - "8888:8888"     # Filer UI
      - "9333:9333"     # Master UI
      - "12535:12535"   # Admin UI
    volumes:
      - ./data:/data
```

| 옵션 | 왜 넣었나 |
| --- | --- |
| `image: chrislusf/seaweedfs:4.50` | 공식 문서가 "mini의 기본값은 릴리즈마다 바뀔 수 있다"고 명시합니다. 태그를 고정해야 재설치 때 같은 동작을 얻습니다 |
| `-dir=/data` | 메타데이터와 볼륨 파일을 둘 위치. 바인드 마운트로 호스트 `./data`에 남깁니다 |
| `-master.defaultReplication=000` | 복제 없음. 단일 노드라 복제할 곳이 없습니다. 명시해 두면 3대 구성에서 `001`로 바꿀 때 어디를 고치는지 분명합니다 |
| `-master.telemetry=false` | 기본값이 익명 통계를 외부로 보냅니다. 사내망 서버에서는 끕니다 |
| `-admin.password=...` | Admin UI는 기본적으로 인증이 없습니다. 비밀번호를 주면 로그인 화면이 생깁니다 |
| `-bucket=lakehouse` | 기동 때 버킷을 만듭니다. 없으면 `weed shell`에서 `s3.bucket.create`로 만듭니다 |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | 첫 관리자 계정입니다. 이 값이 있어야 S3 게이트웨이가 **인증 필수 모드**로 시작합니다. 없으면 누구나 읽고 쓰는 Allow-All 모드입니다 |

Allow-All 모드는 주의할 점이 하나 있습니다. 계정을 하나라도 만들면 인증 필수로 바뀌고, 그 뒤 계정을 모두 지워도 Allow-All로 돌아가지 않습니다. 처음부터 관리자 키를 주고 시작하는 것이 단순합니다.

### 3-2. .env

```bash
mkdir -p ~/seaweedfs && cd ~/seaweedfs
cat > .env <<EOF_ENV
SEAWEEDFS_ROOT_ACCESS_KEY=root
SEAWEEDFS_ROOT_SECRET_KEY=$(openssl rand -hex 12)
SEAWEEDFS_ADMIN_PASSWORD=$(openssl rand -hex 8)
EOF_ENV
chmod 600 .env
```

관리자 키는 계정 생성과 버킷 설정에만 씁니다. 앱과 Trino는 5장에서 만드는 버킷 한정 계정을 씁니다.

## 4. 기동과 확인

```bash
cd ~/seaweedfs
docker compose up -d
docker compose logs -f
```

로그에 다음 순서로 나오면 정상입니다. 10초 안에 끝납니다.

```text
Starting SeaweedFS Mini ...
  Master       starting
  Volume       starting
  Filer        starting
  S3           starting
Added admin identity from AWS environment variables: name=admin-root, accessKey=root
Start Seaweed S3 API Server ... at http port 8333
  S3           ready
```

세 가지를 바로 확인합니다.

```bash
curl -s http://localhost:9333/cluster/status          # {"IsLeader":true,...} 가 나와야 한다
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8333/    # 403 이면 인증 필수 모드. 200 이면 Allow-All 이므로 .env 를 확인
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:12535/   # 307 이면 로그인 화면으로 보내는 것. 비밀번호가 적용됐다
```

브라우저에서 Admin UI는 `http://localhost:12535` (계정 `admin`, 비밀번호는 `.env`), Filer UI는 `http://localhost:8888`입니다.

로그에 `IAM gRPC service on filer is UNAUTHENTICATED` 경고가 한 줄 나옵니다. Filer의 gRPC 포트(18888)로 누구나 계정을 만들 수 있다는 뜻입니다. 이 포트는 compose에서 공개하지 않았으므로 호스트 밖에서는 닿지 않습니다. 개발 VM이라면 8888·9333도 방화벽으로 사내망에 한정합니다.

## 5. 계정과 버킷

관리자 키로 앱을 돌리지 않습니다. 버킷마다 전용 계정을 만들어 그 버킷만 읽고 쓰게 합니다. 이것은 MinIO 구성안의 "서비스별 계정 + 버킷 단위 정책" 원칙과 같습니다.

```bash
docker compose exec -T seaweedfs weed shell <<'EOF_SHELL'
s3.bucket.list
s3.user.provision -name lakehouse -bucket lakehouse -role readwrite
EOF_SHELL
```

출력은 다음과 같습니다. **시크릿 키는 이때 한 번만 보입니다.** 바로 비밀 저장소에 옮깁니다.

```text
  lakehouse	size:0	logical:0	chunk:0
Created policy "lakehouse-lakehouse-readwrite"
Created user "lakehouse" with policy "lakehouse-lakehouse-readwrite" attached

Access Key: <액세스 키>
Secret Key: <시크릿 키>

Save these credentials - the secret key cannot be retrieved later.
```

`s3.user.provision`은 세 가지를 한 번에 합니다. 버킷 한정 정책을 만들고, 사용자와 키를 만들고, 정책을 붙입니다. 역할은 `readonly`, `readwrite`, `admin` 셋입니다. Trino와 Kafka Connect는 파일을 쓰므로 `readwrite`가 필요합니다. 조회 전용 도구를 붙일 때 `readonly` 계정을 하나 더 만듭니다.

만든 계정은 `weed shell`에서 확인합니다.

```bash
docker compose exec -T seaweedfs weed shell <<< "s3.user.list"
# [{"name":"lakehouse","status":"enabled","policies":["lakehouse-lakehouse-readwrite"],"keys":1}]
```

계정과 정책은 Filer 메타데이터에 저장되어 재시작 뒤에도 남습니다. 파일로 관리하려면 `-s3.config=<json>`을 쓰고 `kill -HUP`로 다시 읽힐 수 있지만, 단일 노드에서는 `weed shell`이 더 단순합니다.

## 6. S3 호환 검증 (AWS CLI)

Trino와 Iceberg가 쓰는 S3 기능을 직접 확인합니다. AWS CLI가 없으면 `amazon/aws-cli` 컨테이너를 같은 Docker 네트워크에 붙여 씁니다. 컨테이너 이름이 그대로 호스트 이름이므로 엔드포인트는 `http://seaweedfs:8333`입니다.

```bash
cd ~/seaweedfs
mkdir -p awsdata && echo hello > awsdata/hello.txt && head -c 20000000 /dev/urandom > awsdata/big.bin

# 5장에서 받은 lakehouse 계정 키를 넣는다
export LH_AK=<액세스 키> LH_SK=<시크릿 키>
awsc() { docker run --rm --network seaweedfs_default \
  -e AWS_ACCESS_KEY_ID=$LH_AK -e AWS_SECRET_ACCESS_KEY=$LH_SK -e AWS_DEFAULT_REGION=us-east-1 \
  -v "$PWD/awsdata:/aws" amazon/aws-cli --endpoint-url http://seaweedfs:8333 "$@"; }
```

AWS CLI를 설치했다면 `awsc`를 `aws --endpoint-url http://localhost:8333`으로 바꾸고 환경변수만 같게 둡니다.

| # | 확인하는 것 | 명령 | 기대 결과 |
| --- | --- | --- | --- |
| 1 | 단일 PUT | `awsc s3 cp /aws/hello.txt s3://lakehouse/warehouse/hello.txt` | `upload: ...` |
| 2 | 멀티파트 업로드 (8MB 초과 파일은 CLI가 자동 분할) | `awsc s3 cp /aws/big.bin s3://lakehouse/warehouse/big.bin` | `upload: ...`. Iceberg의 큰 Parquet 쓰기가 이 경로 |
| 3 | ListObjectsV2 | `awsc s3api list-objects-v2 --bucket lakehouse --prefix warehouse/ --query 'Contents[].{Key:Key,Size:Size}' --output table` | 두 객체, `big.bin` 20000000 |
| 4 | 버킷 한정 권한 | `awsc s3 mb s3://other` | `AccessDenied`. lakehouse 계정은 다른 버킷을 못 만든다 |
| 5 | DeleteObjects 일괄 삭제 | `awsc s3api delete-objects --bucket lakehouse --delete 'Objects=[{Key=warehouse/big.bin}]'` | `Deleted` 목록. Iceberg 스냅샷 만료가 이 API 로 파일을 지운다 |

버킷 버저닝은 관리자 키로 켭니다. 실수로 지운 파일을 되살리는 보험이며, Iceberg 스냅샷 보존과는 별개입니다.

```bash
export LH_AK=root LH_SK=$(grep ROOT_SECRET .env | cut -d= -f2)
awsc s3api put-bucket-versioning --bucket lakehouse --versioning-configuration Status=Enabled
awsc s3api get-bucket-versioning --bucket lakehouse     # {"Status": "Enabled"}
```

버저닝을 켠 뒤 5번을 다시 실행하면 결과에 `DeleteMarker: true`가 붙습니다. 파일은 지워진 것처럼 보이지만 이전 버전이 남아 있다는 뜻입니다.

## 7. 재시작과 영속성

```bash
docker compose restart
sleep 5
awsc s3 ls s3://lakehouse/warehouse/     # hello.txt 가 그대로 보여야 한다 (lakehouse 계정 키로)
```

계정, 정책, 버킷, 객체가 모두 `data/`에서 복원됩니다. 컨테이너를 지우고 다시 만들어도(`docker compose down && up -d`) 같습니다. `data/`를 지우면 전부 사라집니다.

볼륨 상태는 `weed shell`로 봅니다. 볼륨 크기 한도는 디스크 용량에 따라 64MB~1GB 사이로 자동 조정됩니다.

```bash
docker compose exec -T seaweedfs weed shell <<< "volume.list"
```

## 8. 막혔을 때

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| `port is already allocated` | 8333·8888·9333·12535 중 하나를 다른 프로세스가 사용 | 2장 명령으로 찾아 멈추거나 compose의 호스트 포트를 바꿈 |
| 모든 S3 요청이 `403` | 키가 틀렸거나 `.env`가 비어 있음 | `docker compose config`로 환경변수가 들어갔는지 확인 |
| `CreateBucket` `AccessDenied` | 버킷 한정 계정으로 다른 버킷을 만들려 함 | 정상 동작. 버킷은 관리자 키 또는 `weed shell s3.bucket.create`로 |
| 8333이 `200`을 돌려줌 | Allow-All 모드. 관리자 키 없이 기동됨 | `.env` 확인 후 `docker compose up -d --force-recreate` |
| 재시작 후 계정이 사라짐 | `./data` 바인드 마운트가 빠져 컨테이너 안에만 저장됨 | compose `volumes` 확인 |

## 9. 다음 단계: 레이크하우스에서 쓰기

Trino, Iceberg REST Catalog, Kafka Connect는 S3 엔드포인트와 키만 바꾸면 그대로 붙습니다. 세 곳 모두 같은 값입니다.

```properties
# Trino lakehouse.properties 의 S3 부분
fs.native-s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=${ENV:LAKEHOUSE_ACCESS_KEY}
s3.aws-secret-key=${ENV:LAKEHOUSE_SECRET_KEY}
```

- `s3.path-style-access=true`는 유지합니다. `bucket.host` 형태의 가상 호스트 주소를 쓰려면 `-s3.domainName`을 따로 설정해야 합니다.
- `s3.region`은 아무 값이어도 되지만 세 곳을 같게 둡니다. 서명 계산에 들어갑니다.
- `weed mini`에는 Iceberg REST Catalog(8181)가 내장되어 있습니다. 카탈로그를 저장소와 분리해 두는 편이 교체와 백업에 유리하므로, 레이크하우스 문서는 별도 카탈로그를 유지합니다.

## 10. weed mini와 운영 구성의 차이

같은 `weed` 바이너리이고 S3 API, 계정 체계, 볼륨 파일 형식도 같습니다. 차이는 무엇을 한 프로세스에 묶었고 어디까지 자동으로 정해 주는가입니다.

| 항목 | weed mini | 운영 구성 (역할 분리, 3대) |
| --- | --- | --- |
| 프로세스 | 1개에 Master·Volume·Filer·S3·Admin UI·유지보수 워커·Iceberg Catalog 전부 | 서버당 Master, Volume, Filer+S3 3개. Admin UI와 워커는 필요할 때 따로 |
| Master | 1개. Raft 없음. 죽으면 저장소 전체 중단 | 3개 Raft. 1대 장애 시 리더 재선출 |
| 복제 | 기본 없음(`000`) | `001` 이상. 서버 1대 장애에도 데이터 유지 |
| Filer 메타데이터 | 로컬 leveldb. Filer 1개 전용 | Postgres 같은 공유 저장소. Filer 3개가 같은 메타데이터를 봄 |
| 볼륨 크기 | 디스크 용량에 따라 64MB~1GB 자동 조정 | 기본 30GB. `-volumeSizeLimitMB`로 명시 |
| Volume 디스크 | `-dir` 하나 | 디스크마다 `-dir` 또는 Volume 프로세스. `-max=0`으로 디스크 용량만큼 |
| 포트 | Volume 9340, S3 8333 | Volume 8080, S3 8333 |
| 계정 | 환경변수 또는 `weed shell`로 생성 | `-s3.config=s3.json` 파일. SIGHUP으로 재로딩 |
| TLS | 없음 | `-s3.cert.file`/`-s3.key.file` 또는 앞단 L4·프록시 |
| 종료 대기 | 1초 | 10초. 쓰기 중인 요청을 다른 서버로 넘길 시간 |
| 설정 | 플래그 몇 개. 나머지는 자동 | `master.toml`, `filer.toml`, `security.toml` 직접 작성 |
| 기본값 | 공식 문서가 "릴리즈마다 바뀔 수 있다"고 명시 | 명시한 값만 적용 |

**S3 Gateway를 Filer와 묶는 이유.** 공식 Production Setup 문서의 "Setup S3 API" 절은 "Start s3 together with filer. This avoids the setup for s3 to support multiple filers."라고 적습니다. S3 Gateway는 상태가 없고 요청마다 Filer에 묻습니다. 같은 프로세스에 두면 항상 자기 Filer를 쓰므로 Filer 주소 등록과 장애 전환 설정이 사라집니다. 그래서 운영은 역할 4개를 프로세스 3개로 띄웁니다.

**mini에서 운영으로 넘어갈 때.** 데이터를 옮기지 않고 운영 클러스터를 새로 만들어 다시 적재하는 것을 권합니다. mini의 leveldb 메타데이터를 Postgres로 옮기려면 `weed filer.meta.backup`과 복원을 거쳐야 합니다. Kafka에 원본 이벤트가 남아 있으면 Connect를 다시 돌리는 쪽이 단순합니다.

## 관련 문서

- [SeaweedFS 토픽](../README.md) — MinIO에서 바꾸는 이유, 작성 예정 문서
- [MinIO: 03 구축 구성안](../../minio/concepts/03-setup-plan.md) — 계정·버킷 체계의 원칙. 저장소만 바뀌고 원칙은 같다
- [Lakehouse: 02 구성요소 정의와 역할](../../lakehouse/concepts/02-components.md) — 저장소가 레이크하우스에서 맡는 역할

## 참고한 공식 문서

- [Quick Start with weed mini](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini)
- [Simplest S3 Bucket and User Setup](https://github.com/seaweedfs/seaweedfs/wiki/Simplest-S3-Bucket-and-User-Setup)
- [S3 Credentials](https://github.com/seaweedfs/seaweedfs/wiki/S3-Credentials)
- [Amazon S3 API](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)
- [Production Setup](https://github.com/seaweedfs/seaweedfs/wiki/Production-Setup)

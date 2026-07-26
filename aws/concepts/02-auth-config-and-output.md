# 인증·설정과 출력 다루기

AWS CLI를 쓰기 전에 "누구로 접속하는지(인증)"와 "어느 리전·어떤 형식으로 받을지(설정·출력)"를 정합니다. 이 두 가지가 CLI 사용의 토대입니다.

## 인증 (자격증명)

CLI는 여러 곳에서 자격증명을 찾습니다(우선순위 대략 아래 순서).

```text
1. 명령 옵션 / 환경변수 (AWS_ACCESS_KEY_ID 등)
2. ~/.aws/credentials, ~/.aws/config 의 프로파일
3. SSO 캐시 (aws sso login)
4. EC2/ECS/EKS 인스턴스·태스크 역할 (IMDS/컨테이너 자격증명)
```

### 기본 설정

```bash
aws configure                 # 액세스 키, 리전, 출력 포맷 입력 → ~/.aws/ 에 저장
aws sts get-caller-identity   # "지금 나는 어떤 계정/역할인가" 확인 (가장 먼저)
```

### 프로파일 — 계정·환경 전환

```bash
aws configure --profile prod          # prod 프로파일 설정
aws s3 ls --profile prod              # 특정 프로파일로 실행
export AWS_PROFILE=prod               # 세션 기본 프로파일 지정
aws configure list-profiles
```

`~/.aws/config` 예시:

```ini
[profile prod]
region = ap-northeast-2
output = json
```

### SSO / 역할 assume

```bash
aws configure sso            # IAM Identity Center(SSO) 로그인 구성 (v2)
aws sso login --profile prod # 브라우저로 로그인
```

- **인스턴스 역할**: EC2/ECS에서 실행하면 키 없이 자동으로 붙은 IAM 역할을 사용합니다. 운영에서는 키를 파일에 두기보다 **역할**을 권장합니다.
- 시크릿은 파일·히스토리에 남기지 말고 SSO·역할·Secret Manager로 주입합니다.

## 출력 다루기

### 출력 포맷

```bash
--output json    # 기본, 스크립트 파싱용
--output table   # 사람이 보기 좋게
--output text    # grep/awk 연동에 좋음
--output yaml    # v2
```

### `--query` (JMESPath) — 필요한 필드만 추출

```bash
aws ec2 describe-instances \
  --query 'Reservations[].Instances[].{ID:InstanceId,State:State.Name,IP:PrivateIpAddress}' \
  --output table
```

- 응답 JSON을 서버가 아니라 **클라이언트에서** 걸러 원하는 모양으로 만듭니다.

### 서버측 필터 vs 클라이언트측 쿼리

```text
--filters : 서버가 걸러서 보냄 (전송량·비용 절감)   예) --filters "Name=instance-state-name,Values=running"
--query   : 받은 결과를 클라이언트에서 가공          예) --query 'Reservations[].Instances[].InstanceId'
```

둘을 함께 쓰면 효율적입니다: 서버 필터로 줄이고, 쿼리로 모양을 잡습니다.

### 페이지네이션·대기

```bash
--page-size 100 --max-items 500     # 대량 결과 제어
aws ec2 wait instance-running --instance-ids i-0abc123   # 상태 충족까지 대기(waiter)
```

### 안전 확인·템플릿

```bash
--dry-run                          # 실제 실행 없이 권한/유효성만 확인(일부 명령)
aws ec2 run-instances --generate-cli-skeleton > params.json   # 입력 JSON 템플릿
```

## 자주 하는 실수

- 리전 미지정 → 엉뚱한 리전 조회. 프로파일·환경변수·`--region`으로 명확히.
- 액세스 키를 코드·저장소에 커밋 → 유출. 역할/SSO/Secret Manager 사용.
- `--query`와 `--filters`를 혼동 → 큰 결과를 다 받아 클라이언트에서 거르며 느려짐.

## 참고한 공식 문서

- [Configuration and credential files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
- [Configure the AWS CLI with IAM Identity Center (SSO)](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html)
- [Controlling command output](https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-output.html)

# AWS CLI 기본 개념

터미널에서 AWS 전체를 조작하는 공식 명령줄 도구입니다. 웹 콘솔에서 클릭하는 작업을 명령어·스크립트로 자동화합니다. Python 기반이고 거의 **모든 AWS 서비스(수백 개)** 를 커버합니다.

```bash
aws <서비스> <작업> [--옵션]
# 예: aws s3 ls / aws ec2 describe-instances / aws lambda invoke ...
```

## 버전 — v1 vs v2

| | v1 | v2 (권장) |
| --- | --- | --- |
| 설치 | pip(Python 의존) | 독립 실행 번들(파이썬 불필요) |
| 신기능 | 유지보수 위주 | SSO 로그인, 자동완성 개선, YAML 출력, `--cli-auto-prompt` 등 |
| 현재 | 레거시 | 신규는 v2 사용 |

## 주요 기능·특징

- **통일된 명령 구조**: `aws <service> <operation> --parameters` 형태로 모든 서비스가 일관됩니다. 각 API가 그대로 명령이 됩니다.
- **완전한 커버리지**: 콘솔로 하는 거의 모든 작업을 명령으로 수행합니다.
- **인증/프로파일**: 액세스 키, SSO, IAM 역할 assume, EC2/ECS 인스턴스 역할, MFA를 지원하고 프로파일로 계정·환경을 전환합니다. (→ [02 문서](02-auth-config-and-output.md))
- **출력 포맷 선택**: `--output json|table|text|yaml`. 스크립트 파싱은 json/text, 사람이 볼 때는 table.
- **`--query`(JMESPath)**: 응답 JSON에서 원하는 필드만 추출·가공.
- **Waiters**: `aws ec2 wait instance-running ...` 처럼 조건 충족까지 대기 → 자동화에 유용.
- **고수준 명령**: `aws s3 sync`, `aws s3 cp --recursive`처럼 편의 명령 제공.
- **안전장치**: `--dry-run`(권한·유효성만 확인), `--generate-cli-skeleton`(입력 템플릿), 방대한 내장 도움말(`aws help`).

좋은 경우:

- 반복 작업을 스크립트로 자동화하고 싶을 때(CI/CD, 배포, 운영).
- 콘솔로 하기 번거로운 대량·일괄 작업.
- `--query`/`jq`로 결과를 프로그래밍적으로 가공해야 할 때.

주의할 점:

- 명령형 도구입니다. **재현 가능한 인프라 상태 관리가 목적이면 IaC(CloudFormation/CDK/Terraform)** 를 함께 씁니다(아래 표 참고).
- 자격증명·시크릿을 히스토리·로그에 남기지 않도록 주의합니다.

## 설치 (v2)

```bash
# Linux x86_64
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip awscliv2.zip && sudo ./aws/install
aws --version
```
(macOS는 `.pkg`, Windows는 MSI 설치 파일 제공)

## 관련 도구와의 위치

| 도구 | 용도 |
| --- | --- |
| **AWS CLI** | 명령어로 AWS 조작·자동화 (이 문서) |
| SDK (boto3 등) | 애플리케이션 코드에서 AWS 호출 |
| CloudFormation/CDK/Terraform | 인프라를 코드로 선언(IaC) |
| CloudShell | 브라우저에서 바로 CLI 실행(설치 불필요) |

CLI는 **명령형**(내가 명령을 내림), IaC는 **선언형**(원하는 상태를 정의)이라 상호 보완적입니다.

## 참고한 공식 문서

- [AWS CLI User Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
- [AWS CLI v2 설치](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

# AWS CLI 서비스별 치트시트

자주 쓰는 명령을 서비스·작업별로 모읍니다. 리전은 서울 `ap-northeast-2` 기준이며, 계정 ID·리소스 ID·시크릿은 예시입니다. 배경은 [01-aws-cli-basics](../concepts/01-aws-cli-basics.md), [02-auth-config-and-output](../concepts/02-auth-config-and-output.md) 참고.

## 시작·신원 확인

```bash
aws configure                          # 키·리전·출력포맷 설정
aws sts get-caller-identity            # 지금 어떤 계정/역할인가 (가장 먼저)
aws configure list-profiles
```

## S3 (스토리지)

```bash
aws s3 ls                                              # 버킷 목록
aws s3 ls s3://my-bucket/logs/                         # 버킷 내부
aws s3 cp ./backup.tar.gz s3://my-bucket/backups/      # 업로드
aws s3 cp s3://my-bucket/data.csv .                    # 다운로드
aws s3 sync ./dist s3://my-bucket/site --delete        # 폴더 동기화(변경분만, 삭제 반영)
aws s3 presign s3://my-bucket/file.zip --expires-in 3600   # 1시간 임시 URL
```

## EC2 (인스턴스)

```bash
# 실행 중 인스턴스를 표로
aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].{ID:InstanceId,Type:InstanceType,IP:PrivateIpAddress}' \
  --output table

aws ec2 start-instances --instance-ids i-0abc123
aws ec2 stop-instances  --instance-ids i-0abc123
aws ec2 wait instance-running --instance-ids i-0abc123   # 켜질 때까지 대기
```

## ECR + Docker (컨테이너 이미지)

```bash
# ECR 로그인 (Docker에 자격증명 전달)
aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin 123456789012.dkr.ecr.ap-northeast-2.amazonaws.com

# 태깅 후 푸시
docker tag myapp:latest 123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/myapp:latest
docker push          123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/myapp:latest
```

## 시크릿·설정

```bash
# Secrets Manager — 비밀번호 조회
aws secretsmanager get-secret-value \
  --secret-id prod/kafka/keystore-password \
  --query SecretString --output text

# SSM Parameter Store — 암호화 파라미터 조회
aws ssm get-parameter --name /prod/db/password --with-decryption \
  --query Parameter.Value --output text
```

## Lambda / CloudWatch Logs

```bash
aws lambda invoke --function-name order-processor --payload '{"id":1}' out.json
aws logs tail /aws/lambda/order-processor --follow      # 실시간 로그 (tail -f 처럼, v2)
```

## IAM (권한)

```bash
aws iam list-users
aws iam list-attached-role-policies --role-name kafka-node-role
```

## 자주 쓰는 패턴

```bash
# ① --query + --output table (사람이 보기 좋게)
aws ec2 describe-volumes --query 'Volumes[].{ID:VolumeId,Size:Size,State:State}' --output table

# ② jq 파이프 (프로그래밍적으로 가공)
aws ec2 describe-instances --output json | jq -r '.Reservations[].Instances[].InstanceId'

# ③ 프로파일로 계정/환경 전환
aws s3 ls --profile prod

# ④ 안전 확인 (권한만 검증, 실제 실행 안 함)
aws ec2 stop-instances --instance-ids i-0abc123 --dry-run

# ⑤ 입력 템플릿 생성 (복잡한 명령)
aws ec2 run-instances --generate-cli-skeleton > params.json
```

## 실전 조합 예시

```bash
# 실행 중인 인스턴스 ID를 모아 순차 정지
for id in $(aws ec2 describe-instances \
    --filters "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].InstanceId' --output text); do
  echo "stopping $id"
  aws ec2 stop-instances --instance-ids "$id"
done
```

## 참고한 공식 문서

- [AWS CLI Command Reference](https://docs.aws.amazon.com/cli/latest/reference/)
- [aws s3 (high-level)](https://docs.aws.amazon.com/cli/latest/userguide/cli-services-s3-commands.html)

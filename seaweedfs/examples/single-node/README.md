# SeaweedFS 단일 노드 예제

`weed mini` 하나로 Master·Volume·Filer·S3 게이트웨이·Admin UI를 한 컨테이너에 띄우는 Compose 파일입니다. 사용 방법과 검증 절차는 [01 단일 노드 설치](../../concepts/01-install-single-node.md)에 있습니다.

```bash
cp .env.example .env            # 값은 openssl rand -hex 12 로 생성
docker compose up -d
docker compose exec -T seaweedfs weed shell <<< "s3.user.provision -name lakehouse -bucket lakehouse -role readwrite"
```

`.env`와 `data/`는 커밋하지 않습니다.

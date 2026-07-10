# Port already in use

## 증상

컨테이너 실행 시 호스트 포트를 사용할 수 없다는 에러가 발생합니다.

## 자주 보는 원인

- 이미 같은 포트를 사용하는 컨테이너가 실행 중임
- 로컬 애플리케이션이 해당 포트를 점유 중임
- 이전 컨테이너가 종료되지 않고 남아 있음

## 확인 방법

```bash
docker ps
docker ps -a
docker ps --filter publish=8080
docker compose port <service> <container-port>
lsof -i :8080
```

## 해결 방법

- 기존 컨테이너를 중지하거나 삭제합니다.
- 호스트 포트 매핑을 다른 값으로 바꿉니다.

```bash
docker run -p 8081:80 nginx
```

포트를 바꾼 뒤에는 애플리케이션의 callback URL, 방화벽, healthcheck, 문서화된 접속 주소도 함께 수정됐는지 확인합니다. 로컬 전용 서비스라면 `127.0.0.1:8081:80`처럼 바인딩 범위를 제한합니다.

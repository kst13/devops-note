# Container exits immediately

## 증상

컨테이너가 실행 직후 바로 종료됩니다.

## 자주 보는 원인

- 컨테이너의 메인 프로세스가 종료됨
- `CMD` 또는 `ENTRYPOINT`가 잘못됨
- 애플리케이션 실행 중 에러가 발생함

## 확인 방법

```bash
docker ps -a
docker logs <container>
docker inspect <container>
```

## 해결 방법

- 로그에서 애플리케이션 에러를 먼저 확인합니다.
- 컨테이너가 foreground 프로세스를 실행하는지 확인합니다.
- 디버깅이 필요하면 쉘로 진입해 실행 명령을 직접 확인합니다.

```bash
docker run -it --entrypoint sh <image>
```

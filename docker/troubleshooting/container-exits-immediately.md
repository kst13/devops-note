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
docker inspect <container> --format '{{json .State}}'
```

`State.ExitCode`, `State.Error`, `State.OOMKilled`, health 상태를 함께 확인합니다. 자주 보는 종료 코드는 다음과 같습니다.

| 코드 | 일반적인 의미 |
| --- | --- |
| `125` | Docker가 컨테이너 실행 자체에 실패 |
| `126` | 명령은 있지만 실행 권한 또는 형식 문제 |
| `127` | 실행할 명령을 찾지 못함 |
| `137` | `SIGKILL`, OOM 또는 강제 종료 가능성 |

종료 코드만으로 원인을 확정하지 말고 로그와 `OOMKilled`를 같이 봅니다.

## 해결 방법

- 로그에서 애플리케이션 에러를 먼저 확인합니다.
- 컨테이너가 foreground 프로세스를 실행하는지 확인합니다.
- 디버깅이 필요하면 쉘로 진입해 실행 명령을 직접 확인합니다.

```bash
docker run -it --entrypoint sh <image>
```

distroless나 scratch 기반 이미지에는 `sh`가 없을 수 있습니다. 그때는 debug 이미지, `docker cp`, 애플리케이션 자체 진단 endpoint를 사용합니다.

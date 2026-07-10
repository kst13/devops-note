# 컨테이너 운영 기초

## 학습 목표

- CPU와 메모리 사용량을 제한하고 실제 적용값을 확인합니다.
- 프로세스 상태와 애플리케이션 준비 상태를 구분합니다.
- 로그, 재시작 정책, 종료 신호를 운영 관점에서 연결합니다.

## 리소스 제한

컨테이너는 기본적으로 호스트가 허용하는 만큼 CPU와 메모리를 사용할 수 있습니다. 운영에서는 부하 테스트를 바탕으로 제한값과 모니터링 기준을 함께 정합니다.

```bash
docker run -d \
  --name limited-web \
  --memory 256m \
  --cpus 0.5 \
  nginx:alpine

docker stats limited-web
docker inspect limited-web --format '{{json .HostConfig.Memory}} {{json .HostConfig.NanoCpus}}'
```

메모리 제한을 넘으면 Linux OOM 처리로 컨테이너 프로세스가 종료될 수 있습니다. `docker inspect`의 `.State.OOMKilled`와 종료 코드를 함께 확인합니다.

## 프로세스 상태와 healthcheck

프로세스가 실행 중이라는 사실만으로 애플리케이션이 요청을 처리할 준비가 됐다고 볼 수 없습니다. `HEALTHCHECK`는 컨테이너 안에서 애플리케이션 상태를 검사합니다.

```dockerfile
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q --spider http://127.0.0.1/ || exit 1
```

```bash
docker inspect limited-web --format '{{json .State.Health}}'
docker events --filter container=limited-web
```

검사 명령에 사용하는 `wget`이나 `curl`이 이미지 안에 실제로 있어야 합니다. 일반 Docker 실행은 `unhealthy` 상태만 기록하며 컨테이너를 자동 재시작하지 않습니다. 재시작 여부는 오케스트레이터나 별도 정책에서 결정합니다.

## 재시작 정책

| 정책 | 의미 | 주의점 |
| --- | --- | --- |
| `no` | 자동 재시작하지 않음 | 기본값 |
| `on-failure[:N]` | 비정상 종료 시 재시작 | 정상 종료에는 적용되지 않음 |
| `always` | daemon 재시작 후에도 다시 실행 | 수동 중지 동작을 이해해야 함 |
| `unless-stopped` | 수동으로 중지하지 않았다면 재실행 | 일반 서버 워크로드에 자주 사용 |

```bash
docker update --restart unless-stopped limited-web
docker inspect limited-web --format '{{.HostConfig.RestartPolicy.Name}}'
```

재시작 정책은 반복 장애의 원인을 해결하지 않습니다. 재시작 횟수와 애플리케이션 로그를 함께 관찰합니다.

## 로그와 디스크 사용량

애플리케이션 로그는 파일보다 표준 출력과 표준 오류로 보내는 편이 컨테이너 로그 수집에 유리합니다.

```bash
docker logs --tail 100 -f limited-web
docker inspect limited-web --format '{{.HostConfig.LogConfig.Type}}'
```

기본 `json-file` 로그 드라이버는 별도 설정이 없으면 자동 순환하지 않습니다. 단일 컨테이너 실행에서는 다음처럼 크기와 파일 수를 제한할 수 있습니다.

```bash
docker run -d \
  --name rotated-web \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  nginx:alpine
```

## 정상 종료와 PID 1

`docker stop`은 먼저 종료 신호를 보내고 유예 시간 뒤 강제 종료합니다. 애플리케이션이 신호를 받으려면 `CMD`나 `ENTRYPOINT`의 exec form을 사용해 실행 파일이 PID 1이 되도록 합니다.

```dockerfile
CMD ["node", "server.js"]
```

쉘 스크립트를 진입점으로 쓴다면 마지막 프로세스를 `exec`로 실행합니다.

```sh
exec "$@"
```

## 운영 확인 순서

```bash
docker ps -a
docker inspect limited-web --format '{{json .State}}'
docker stats --no-stream limited-web
docker logs --tail 100 limited-web
docker top limited-web
```

상태 → 종료 코드/OOM → 리소스 → 로그 → 프로세스 순으로 확인하면 원인 범위를 빠르게 줄일 수 있습니다.

## 정리

```bash
docker rm -f limited-web rotated-web
```

## 참고한 공식 문서

- [Resource constraints](https://docs.docker.com/engine/containers/resource_constraints/)
- [Dockerfile HEALTHCHECK](https://docs.docker.com/reference/dockerfile/#healthcheck)
- [Configure logging drivers](https://docs.docker.com/engine/logging/configure/)
- [Start containers automatically](https://docs.docker.com/engine/containers/start-containers-automatically/)

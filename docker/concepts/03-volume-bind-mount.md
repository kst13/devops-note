# 볼륨과 바인드 마운트

## 학습 목표

- 컨테이너 writable layer, volume, bind mount, tmpfs의 수명을 구분합니다.
- 데이터 성격에 맞는 마운트 방식을 선택합니다.
- 마운트 권한과 기존 파일 가림 현상을 진단합니다.

## 방식 비교

| 방식 | 데이터 위치 | 적합한 용도 | 주의점 |
| --- | --- | --- | --- |
| 컨테이너 writable layer | Docker 내부 | 임시 파일, 재생성 가능한 데이터 | 컨테이너 삭제 시 제거 |
| named volume | Docker가 관리 | DB 데이터, 컨테이너 생성 데이터 | 별도 백업이 필요 |
| bind mount | 지정한 호스트 경로 | 소스 코드, 설정, 호스트와 파일 공유 | 호스트 경로와 UID/GID에 의존 |
| tmpfs | 호스트 메모리 | 디스크에 남기지 않을 임시 데이터 | 중지 시 사라지고 메모리 사용 |

바인드 마운트도 컨테이너와 독립적으로 유지됩니다. 다만 애플리케이션이 생성하는 영속 데이터는 호스트 경로 결합도가 낮고 Docker가 수명주기를 관리하는 named volume을 기본 선택으로 검토합니다.

## named volume 실습

```bash
docker volume create app-data

docker run --rm \
  --mount type=volume,src=app-data,dst=/data \
  alpine sh -c 'date -u > /data/created-at'

docker run --rm \
  --mount type=volume,src=app-data,dst=/data,readonly \
  alpine cat /data/created-at

docker volume inspect app-data
```

첫 컨테이너는 종료 후 자동 삭제되지만 두 번째 컨테이너가 같은 파일을 읽을 수 있습니다. 데이터 수명이 컨테이너와 분리됐기 때문입니다.

## bind mount 실습

```bash
mkdir -p ./mount-demo

docker run --rm \
  --mount type=bind,src="$(pwd)/mount-demo",dst=/workspace \
  alpine sh -c 'echo created-in-container > /workspace/result.txt'

cat ./mount-demo/result.txt
```

읽기 전용 설정 파일은 쓰기 권한을 주지 않습니다.

```bash
docker run --rm \
  --mount type=bind,src="$(pwd)/config",dst=/app/config,readonly \
  my-app
```

## 자주 놓치는 점

- 비어 있지 않은 컨테이너 경로에 마운트하면 기존 파일이 삭제되는 것이 아니라 마운트 뒤에 가려집니다.
- 컨테이너 프로세스의 숫자 UID/GID와 호스트 파일 소유자가 다르면 `Permission denied`가 발생할 수 있습니다.
- bind mount는 호스트 파일을 변경할 수 있으므로 쓰기 권한 범위를 최소화합니다.
- volume은 백업이 아닙니다. 별도 저장 위치로 내보내고 복원 절차를 주기적으로 시험합니다.
- `docker compose down -v`와 `docker volume prune`은 데이터를 삭제할 수 있으므로 대상 확인 없이 실행하지 않습니다.

## 확인 명령어

```bash
docker volume ls
docker volume inspect app-data
docker inspect <container> --format '{{json .Mounts}}'
docker system df -v
```

## 정리

```bash
docker volume rm app-data
rm -r ./mount-demo
```

## 함께 보면 좋은 문서

- [Permission denied](../troubleshooting/permission-denied.md)
- [Compose 기본 실습](../examples/compose-basic/README.md)

## 참고한 공식 문서

- [Volumes](https://docs.docker.com/engine/storage/volumes/)
- [Bind mounts](https://docs.docker.com/engine/storage/bind-mounts/)
- [tmpfs mounts](https://docs.docker.com/engine/storage/tmpfs/)

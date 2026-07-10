# Docker 시작하기

## 학습 목표

- Docker CLI, Docker daemon, registry의 역할을 구분합니다.
- 이미지로 컨테이너를 만들고 상태를 확인한 뒤 안전하게 정리합니다.
- 명령이 실패했을 때 가장 먼저 확인할 위치를 익힙니다.

## 사전 확인

Docker Desktop 또는 Docker Engine과 Compose v2가 필요합니다.

```bash
docker version
docker info
docker compose version
```

`docker version`에서 Client와 Server가 모두 보여야 합니다. Client만 보인다면 CLI는 설치됐지만 Docker daemon에 연결하지 못한 상태입니다.

## Docker가 동작하는 흐름

```text
Docker CLI
  -> Docker daemon
     -> registry에서 이미지 pull
     -> 이미지로 컨테이너 생성
     -> 호스트 커널에서 프로세스 실행
```

- **CLI**는 `docker run`, `docker ps` 같은 요청을 보냅니다.
- **daemon**은 이미지, 컨테이너, 네트워크, 볼륨을 실제로 관리합니다.
- **registry**는 이미지를 저장하고 배포합니다. Docker Hub가 대표적인 공개 registry입니다.
- **컨테이너**는 별도 가상 머신이 아니라 호스트 커널 위에서 격리된 프로세스로 실행됩니다.

## 첫 컨테이너 실행

```bash
docker run -d \
  --name hello-nginx \
  -p 127.0.0.1:8080:80 \
  nginx:alpine
```

명령은 이미지가 로컬에 없으면 먼저 내려받고, 컨테이너를 생성한 뒤 백그라운드에서 실행합니다. `127.0.0.1:8080:80`은 호스트의 로컬 8080 포트를 컨테이너의 80 포트에 연결합니다.

```bash
docker ps
curl http://127.0.0.1:8080
docker logs hello-nginx
docker inspect hello-nginx
```

`curl` 결과에 Nginx HTML이 보이고 `docker ps`의 상태가 `Up`이면 정상입니다.

## 생명주기 확인

```bash
docker stop hello-nginx
docker ps -a
docker start hello-nginx
docker rm -f hello-nginx
docker images nginx
```

컨테이너를 삭제해도 이미지는 남습니다. 같은 이미지에서 컨테이너를 다시 만들 수 있기 때문입니다. 이미지까지 지우려면 사용 중인 컨테이너가 없는지 확인한 뒤 실행합니다.

```bash
docker image rm nginx:alpine
```

## 확인 질문

1. `docker run`과 `docker start`는 어떤 점이 다른가요?
2. 컨테이너를 삭제해도 이미지가 남는 이유는 무엇인가요?
3. `docker version`에 Server 정보가 없으면 어느 계층부터 확인해야 하나요?

## 다음 문서

- [컨테이너와 이미지](01-container-image.md)
- [Dockerfile](02-dockerfile.md)

## 참고한 공식 문서

- [Docker architecture](https://docs.docker.com/get-started/docker-overview/)
- [Run containers](https://docs.docker.com/guides/golang/run-containers/)

# Docker CLI 치트시트

## 컨테이너

```bash
docker run <image>
docker run -d --name <name> <image>
docker ps
docker ps -a
docker stop <container>
docker start <container>
docker restart <container>
docker rm <container>
docker logs <container>
docker logs --tail 100 -f <container>
docker exec -it <container> sh
docker inspect <container> --format '{{json .State}}'
docker stats --no-stream <container>
docker top <container>
```

## 이미지

```bash
docker images
docker pull <image>
docker build -t <name>:<tag> .
docker rmi <image>
docker history <image>
```

## 볼륨

```bash
docker volume ls
docker volume create <name>
docker volume inspect <name>
docker volume rm <name>
```

## 네트워크

```bash
docker network ls
docker network create <name>
docker network inspect <name>
docker network connect <network> <container>
docker network disconnect <network> <container>
docker network rm <name>
```

## Compose

```bash
docker compose up
docker compose up -d
docker compose config -q
docker compose down
docker compose logs
docker compose logs -f --tail 100
docker compose ps
docker compose exec <service> sh
docker compose port <service> <container-port>
```

`docker compose down -v`는 named volume과 anonymous volume 데이터를 삭제할 수 있습니다. 복구할 데이터가 없는지 확인한 뒤 실행합니다.

## 운영 진단

```bash
docker system df -v
docker events --since 10m
docker ps -a --filter exited=137
docker ps --filter publish=8080
docker inspect <container> --format '{{json .Mounts}}'
docker inspect <container> --format '{{json .NetworkSettings.Networks}}'
```

## 정리 명령 주의

다음 명령은 사용하지 않는 리소스나 데이터를 대량 삭제할 수 있습니다. 자동화 전에 대상 목록과 복구 가능 여부를 확인합니다.

```text
docker system prune
docker image prune -a
docker volume prune
docker compose down -v
```

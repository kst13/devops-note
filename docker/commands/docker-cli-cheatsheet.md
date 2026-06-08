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
docker exec -it <container> sh
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
docker network rm <name>
```

## Compose

```bash
docker compose up
docker compose up -d
docker compose down
docker compose down -v
docker compose logs
docker compose ps
```

# Docker Compose

## 핵심 개념

Docker Compose는 여러 컨테이너를 하나의 YAML 파일로 정의하고 함께 실행하는 도구입니다.

## 기본 예시

```yaml
services:
  web:
    image: nginx:alpine
    ports:
      - "8080:80"
```

## 자주 쓰는 명령어

```bash
docker compose up
docker compose up -d
docker compose ps
docker compose logs
docker compose down
docker compose down -v
```

## 정리할 것

- `services`, `ports`, `volumes`, `environment` 사용법
- `docker compose down`과 `docker compose down -v`의 차이
- 여러 서비스 간 통신 방식

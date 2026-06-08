# Dockerfile

## 핵심 개념

Dockerfile은 이미지를 만들기 위한 빌드 명세입니다. 베이스 이미지, 복사할 파일, 설치할 패키지, 실행 명령을 선언합니다.

## 기본 예시

```dockerfile
FROM nginx:alpine
COPY ./html /usr/share/nginx/html
EXPOSE 80
```

## 자주 쓰는 명령어

```bash
docker build -t my-nginx .
docker run --name my-web -p 8080:80 my-nginx
docker history my-nginx
```

## 정리할 것

- `FROM`, `COPY`, `RUN`, `CMD`, `ENTRYPOINT`의 차이
- 빌드 캐시가 동작하는 방식
- 이미지 레이어 개념

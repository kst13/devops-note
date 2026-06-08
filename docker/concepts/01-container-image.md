# 컨테이너와 이미지

## 핵심 개념

- 이미지는 애플리케이션 실행에 필요한 파일, 설정, 의존성을 묶은 읽기 전용 템플릿입니다.
- 컨테이너는 이미지를 실행한 프로세스입니다.
- 같은 이미지로 여러 컨테이너를 만들 수 있습니다.

## 자주 쓰는 명령어

```bash
docker images
docker pull nginx
docker run --name web -p 8080:80 nginx
docker ps
docker ps -a
docker stop web
docker rm web
```

## 실습

```bash
docker run --name hello-nginx -p 8080:80 nginx
```

브라우저에서 `http://localhost:8080`에 접속해 Nginx 기본 페이지가 보이는지 확인합니다.

## 정리할 것

- 이미지와 컨테이너의 차이
- `docker run`이 내부적으로 수행하는 일
- 컨테이너 삭제와 이미지 삭제의 차이

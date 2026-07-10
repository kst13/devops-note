# Dockerfile 기본 실습

정적 HTML을 Nginx 이미지에 넣고 빌드, 실행, 상태 확인, 정리까지 수행합니다.

## 파일 구성

```text
dockerfile-basic/
├── .dockerignore
├── Dockerfile
├── README.md
└── html/
    └── index.html
```

## 빌드

이 디렉터리에서 실행합니다.

```bash
docker build -t dockerfile-basic:local .
docker image inspect dockerfile-basic:local
docker history dockerfile-basic:local
```

## 실행과 검증

```bash
docker run -d \
  --name dockerfile-basic \
  -p 127.0.0.1:8080:80 \
  dockerfile-basic:local

curl http://127.0.0.1:8080
docker ps --filter name=dockerfile-basic
docker inspect dockerfile-basic --format '{{json .State.Health}}'
```

HTML에 `Dockerfile basic example`이 보이고 health 상태가 `healthy`가 되면 성공입니다. 처음 몇 초 동안은 `starting`일 수 있습니다.

## 캐시 확인

아무 파일도 바꾸지 않고 다시 빌드합니다.

```bash
docker build -t dockerfile-basic:local .
```

출력의 빌드 단계에 `CACHED`가 표시되는지 확인합니다. 그다음 `html/index.html`을 수정해 `COPY` 단계 이후의 캐시만 무효화되는지 비교합니다.

## 정리

```bash
docker rm -f dockerfile-basic
docker image rm dockerfile-basic:local
```

## 확인 질문

1. `.dockerignore`에 불필요한 파일을 넣는 이유는 무엇인가요?
2. `EXPOSE 80`만으로 호스트의 8080 포트에서 접속할 수 있나요?
3. HTML 변경 후 모든 레이어가 다시 빌드되지 않는 이유는 무엇인가요?

# Dockerfile

## 학습 목표

- 빌드 시점 명령과 컨테이너 실행 시점 명령을 구분합니다.
- 빌드 컨텍스트, 레이어, 캐시가 이미지 빌드에 미치는 영향을 이해합니다.
- 크기, 보안, 재현성을 고려한 Dockerfile 점검 기준을 익힙니다.

## 핵심 개념

Dockerfile은 이미지를 만들기 위한 빌드 명세입니다. `docker build` 마지막 인자인 `.`은 현재 디렉터리를 **빌드 컨텍스트**로 전달한다는 의미입니다. `COPY`는 이 컨텍스트 안의 파일만 읽을 수 있습니다.

| 명령 | 실행 시점 | 역할 |
| --- | --- | --- |
| `FROM` | 빌드 | 베이스 이미지 또는 새 빌드 단계 시작 |
| `WORKDIR` | 빌드 | 이후 명령의 기본 경로 설정 |
| `COPY` | 빌드 | 컨텍스트의 파일을 이미지에 복사 |
| `RUN` | 빌드 | 패키지 설치, 컴파일 등으로 레이어 생성 |
| `USER` | 빌드/실행 설정 | 이후 명령과 기본 실행 사용자 지정 |
| `EXPOSE` | 이미지 메타데이터 | 애플리케이션이 듣는 포트 문서화 |
| `CMD` | 컨테이너 시작 | 기본 실행 명령 또는 기본 인자 제공 |
| `ENTRYPOINT` | 컨테이너 시작 | 항상 실행할 프로그램 지정 |

`EXPOSE`는 호스트 포트를 열지 않습니다. 외부 접근에는 `docker run -p` 또는 Compose `ports`가 필요합니다.

## 기본 예시

```dockerfile
FROM nginx:alpine

COPY html/index.html /usr/share/nginx/html/index.html

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -q --spider http://127.0.0.1/ || exit 1
```

실행 가능한 전체 파일은 [Dockerfile 기본 실습](../examples/dockerfile-basic/README.md)에 있습니다.

## `CMD`와 `ENTRYPOINT`

OS 종료 신호가 애플리케이션에 직접 전달되도록 JSON 배열인 exec form을 기본으로 사용합니다.

```dockerfile
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
CMD ["--spring.profiles.active=prod"]
```

`docker run image --server.port=9090`처럼 실행하면 `ENTRYPOINT`는 유지되고 `CMD` 기본 인자가 바뀝니다. `CMD`만 정의한 이미지에서는 `docker run` 뒤의 명령이 기본 명령 전체를 대체합니다.

## 캐시를 활용하는 복사 순서

자주 바뀌지 않는 의존성 정의를 애플리케이션 소스보다 먼저 복사하면 소스 변경 시 의존성 설치 레이어를 재사용할 수 있습니다.

```dockerfile
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build
```

`.dockerignore`로 `.git`, 빌드 결과, 로컬 의존성, 시크릿처럼 불필요하거나 민감한 파일을 컨텍스트에서 제외합니다.

## 운영 점검 기준

- 빌드 도구가 최종 이미지에 필요 없다면 multi-stage build로 실행 파일만 복사합니다.
- 애플리케이션이 특권을 요구하지 않으면 전용 non-root 사용자로 실행합니다.
- 관련 패키지 설치와 캐시 정리를 같은 `RUN` 단계에서 처리합니다.
- 베이스 이미지는 검증한 버전과 digest를 기록하고 정기적으로 갱신합니다.
- 비밀번호와 토큰을 `ARG`, `ENV`, `COPY`로 이미지 레이어에 넣지 않습니다. BuildKit secret 또는 배포 환경의 시크릿 기능을 사용합니다.
- `latest` 같은 이동 가능한 태그만으로 배포 재현성을 보장하지 않습니다.

## 빌드와 검사

```bash
docker build --check .
docker build -t my-app:local .
docker history my-app:local
docker image inspect my-app:local
```

`docker build --check` 지원 여부는 Docker/BuildKit 버전에 따라 다를 수 있습니다.

## 확인 질문

1. `RUN npm start`와 `CMD ["npm", "start"]`는 언제 실행되나요?
2. 소스 코드보다 의존성 파일을 먼저 `COPY`하는 이유는 무엇인가요?
3. `EXPOSE 8080`만 작성한 이미지가 호스트에 자동 공개되지 않는 이유는 무엇인가요?

## 참고한 공식 문서

- [Dockerfile reference](https://docs.docker.com/reference/dockerfile/)
- [Building best practices](https://docs.docker.com/build/building/best-practices/)
- [Build secrets](https://docs.docker.com/build/building/secrets/)

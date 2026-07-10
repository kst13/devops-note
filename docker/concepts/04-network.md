# Docker 네트워크

## 학습 목표

- 컨테이너 간 통신과 호스트 포트 공개를 구분합니다.
- 사용자 정의 bridge 네트워크의 DNS를 사용합니다.
- 연결 실패를 네트워크, 이름 해석, 리슨 포트 순서로 확인합니다.

## 통신 경로 구분

| 요청 경로 | 주소 예시 | 필요한 설정 |
| --- | --- | --- |
| 같은 사용자 정의 네트워크의 컨테이너끼리 | `http://api:8080` | 같은 네트워크와 대상 프로세스의 리슨 |
| 호스트에서 컨테이너로 | `http://127.0.0.1:8080` | `-p 127.0.0.1:8080:80` |
| 컨테이너에서 외부 인터넷으로 | `https://example.com` | 기본적으로 outbound 가능 |
| 컨테이너에서 호스트 서비스로 | `host.docker.internal` | 플랫폼별 지원 또는 host-gateway 설정 |

사용자 정의 bridge 네트워크에 연결된 컨테이너는 컨테이너명이나 network alias를 DNS 이름으로 사용할 수 있습니다. IP는 재생성 시 달라질 수 있으므로 애플리케이션 설정에 고정하지 않습니다.

## 컨테이너 간 통신 실습

```bash
docker network create learn-net

docker run -d \
  --name web \
  --network learn-net \
  nginx:alpine

docker run --rm \
  --network learn-net \
  curlimages/curl \
  -fsS http://web
```

Nginx HTML이 보이면 Docker DNS가 `web`을 컨테이너 주소로 해석하고 80 포트로 연결한 것입니다. 컨테이너 간 통신에는 `EXPOSE`나 `-p`가 필수가 아닙니다.

```bash
docker network inspect learn-net
docker exec web hostname -i
```

## 포트 공개

```bash
docker run -d --name public-web -p 127.0.0.1:8080:80 nginx:alpine
```

포트 표기는 `호스트 주소:호스트 포트:컨테이너 포트` 순서입니다. `-p 8080:80`처럼 호스트 주소를 생략하면 기본적으로 모든 호스트 인터페이스에 공개될 수 있습니다. 로컬 실습이나 내부 관리 도구는 `127.0.0.1` 바인딩을 우선 검토합니다.

`EXPOSE 80`은 이미지가 사용하는 포트를 설명하는 메타데이터이며 방화벽이나 공개 설정이 아닙니다.

## 연결 실패 확인 순서

```bash
docker inspect <container> --format '{{json .NetworkSettings.Networks}}'
docker network inspect <network>
docker exec <source> getent hosts <service-name>
docker exec <target> ss -lnt
```

1. 두 컨테이너가 같은 네트워크에 있는지 확인합니다.
2. 서비스명이 DNS로 해석되는지 확인합니다.
3. 대상 프로세스가 예상 포트에서 듣는지 확인합니다.
4. 대상이 컨테이너 안의 `127.0.0.1`에만 바인딩됐다면 `0.0.0.0` 리슨이 필요한지 검토합니다.

이미지에 `getent`나 `ss`가 없을 수 있습니다. 그때는 같은 네트워크에 진단용 임시 컨테이너를 실행합니다.

## 정리

```bash
docker rm -f web public-web
docker network rm learn-net
```

## 함께 보면 좋은 문서

- [컨테이너에서 localhost로 다른 서비스 접근하기](06-container-localhost-access.md)
- [Cannot connect between containers](../troubleshooting/cannot-connect-between-containers.md)

## 참고한 공식 문서

- [Networking overview](https://docs.docker.com/engine/network/)
- [Bridge network driver](https://docs.docker.com/engine/network/drivers/bridge/)
- [Port publishing](https://docs.docker.com/engine/network/port-publishing/)

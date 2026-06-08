# Docker 네트워크

## 핵심 개념

Docker 컨테이너는 네트워크를 통해 서로 통신합니다. 같은 사용자 정의 브리지 네트워크에 있는 컨테이너끼리는 컨테이너 이름으로 접근할 수 있습니다.

## 자주 쓰는 명령어

```bash
docker network ls
docker network create app-network
docker network inspect app-network
docker run --network app-network --name app alpine
docker network rm app-network
```

## 정리할 것

- 기본 `bridge` 네트워크와 사용자 정의 네트워크의 차이
- 컨테이너 이름으로 통신하는 방식
- 호스트 포트와 컨테이너 포트의 차이
- 컨테이너 안에서 `localhost`가 가리키는 대상

## 함께 보면 좋은 문서

- [컨테이너에서 localhost로 다른 서비스 접근하기](06-container-localhost-access.md)

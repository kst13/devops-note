# Cannot connect between containers

## 증상

컨테이너 A에서 컨테이너 B로 접속할 수 없습니다.

## 자주 보는 원인

- 두 컨테이너가 같은 네트워크에 있지 않음
- `localhost`를 잘못 사용함
- 대상 서비스가 컨테이너 내부에서만 다른 포트로 실행 중임

## 확인 방법

```bash
docker network ls
docker network inspect <network>
docker exec -it <container> sh
```

## 해결 방법

- 두 컨테이너를 같은 사용자 정의 네트워크에 연결합니다.
- 컨테이너 간 접속에는 `localhost` 대신 서비스명이나 컨테이너명을 사용합니다.
- Compose 환경에서는 서비스명을 호스트명으로 사용합니다.

## 함께 보면 좋은 문서

- [컨테이너에서 localhost로 다른 서비스 접근하기](../concepts/06-container-localhost-access.md)

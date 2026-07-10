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
docker inspect <container> --format '{{json .NetworkSettings.Networks}}'
```

가능하면 source와 같은 네트워크에서 다음 순서로 확인합니다.

```bash
getent hosts <service-name>
nc -vz <service-name> <port>
```

대상 컨테이너에서는 프로세스가 실제로 어느 주소와 포트에서 듣는지 확인합니다.

```bash
ss -lntp
```

`127.0.0.1:<port>`에만 바인딩된 프로세스는 다른 컨테이너 요청을 받지 못합니다. 필요한 경우 `0.0.0.0:<port>`에서 듣도록 애플리케이션을 설정합니다. 이미지에 진단 도구가 없으면 같은 네트워크에 임시 진단 컨테이너를 실행합니다.

## 해결 방법

- 두 컨테이너를 같은 사용자 정의 네트워크에 연결합니다.
- 컨테이너 간 접속에는 `localhost` 대신 서비스명이나 컨테이너명을 사용합니다.
- Compose 환경에서는 서비스명을 호스트명으로 사용합니다.
- 호스트에 공개한 포트가 아니라 대상 컨테이너가 실제로 리슨하는 내부 포트를 사용합니다.

## 함께 보면 좋은 문서

- [컨테이너에서 localhost로 다른 서비스 접근하기](../concepts/06-container-localhost-access.md)

# Docker Study

Docker의 기본 개념, 실습 예제, 트러블슈팅 기록을 정리하는 저장소입니다.

## 정리 원칙

- 개념은 짧게 정의하고, 직접 확인할 수 있는 명령어를 함께 적습니다.
- 트러블슈팅은 증상, 원인, 확인 방법, 해결 방법 순서로 기록합니다.
- 실습 예제는 재현 가능한 최소 구성으로 유지합니다.
- 새로 알게 된 명령어는 `commands/`에 모아 나중에 빠르게 찾아볼 수 있게 합니다.

## 문서 구조

```text
concepts/          Docker 핵심 개념 정리
troubleshooting/   자주 만나는 문제와 해결 기록
commands/          Docker CLI 명령어 치트시트
examples/          직접 실행해볼 수 있는 예제
```

## 추천 학습 순서

1. [컨테이너와 이미지](concepts/01-container-image.md)
2. [Dockerfile](concepts/02-dockerfile.md)
3. [볼륨과 바인드 마운트](concepts/03-volume-bind-mount.md)
4. [Docker 네트워크](concepts/04-network.md)
5. [Docker Compose](concepts/05-compose.md)
6. [컨테이너에서 localhost로 다른 서비스 접근하기](concepts/06-container-localhost-access.md)
7. [Docker CLI 치트시트](commands/docker-cli-cheatsheet.md)

## 트러블슈팅 기록

- [Permission denied](troubleshooting/permission-denied.md)
- [Port already in use](troubleshooting/port-already-in-use.md)
- [Container exits immediately](troubleshooting/container-exits-immediately.md)
- [Cannot connect between containers](troubleshooting/cannot-connect-between-containers.md)

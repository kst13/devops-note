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

### 입문

1. [Docker 시작하기](concepts/00-getting-started.md) — 설치 상태와 전체 동작 흐름 확인
2. [컨테이너와 이미지](concepts/01-container-image.md) — 이미지, 컨테이너, 레이어 구분
3. [Dockerfile](concepts/02-dockerfile.md) — 재현 가능한 이미지 빌드

### 핵심

4. [볼륨과 바인드 마운트](concepts/03-volume-bind-mount.md) — 데이터 수명과 마운트 선택
5. [Docker 네트워크](concepts/04-network.md) — 컨테이너 DNS와 포트 공개
6. [Docker Compose](concepts/05-compose.md) — 여러 서비스를 선언적으로 실행
7. [컨테이너에서 localhost로 다른 서비스 접근하기](concepts/06-container-localhost-access.md) — 대상별 주소 선택

### 실습과 운영

8. [Dockerfile 기본 실습](examples/dockerfile-basic/README.md)
9. [Compose 기본 실습](examples/compose-basic/README.md)
10. [컨테이너 운영 기초](concepts/07-container-operations.md) — 리소스, healthcheck, 로그, 종료
11. [Docker CLI 치트시트](commands/docker-cli-cheatsheet.md)

## 트러블슈팅 기록

- [Permission denied](troubleshooting/permission-denied.md)
- [Port already in use](troubleshooting/port-already-in-use.md)
- [Container exits immediately](troubleshooting/container-exits-immediately.md)
- [Cannot connect between containers](troubleshooting/cannot-connect-between-containers.md)

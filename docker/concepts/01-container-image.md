# 컨테이너와 이미지

## 학습 목표

- 이미지, 컨테이너, 컨테이너 writable layer를 구분합니다.
- `create`, `start`, `run`, `rm`의 관계를 설명합니다.
- 태그가 고정된 식별자가 아니라는 점을 이해합니다.

## 핵심 개념

| 대상 | 역할 | 변경 가능 여부 | 수명 |
| --- | --- | --- | --- |
| 이미지 | 실행 파일, 라이브러리, 기본 설정을 묶은 템플릿 | 읽기 전용 레이어 | 직접 삭제할 때까지 유지 |
| 컨테이너 | 이미지의 설정으로 실행되는 격리된 프로세스 | 실행 상태와 설정 변경 가능 | 삭제할 때까지 유지 |
| writable layer | 컨테이너가 실행 중 기록한 파일 변경분 | 쓰기 가능 | 컨테이너 삭제 시 함께 제거 |

같은 이미지에서 여러 컨테이너를 만들 수 있으며 각 컨테이너의 writable layer는 서로 공유되지 않습니다. 영속 데이터는 writable layer에 의존하지 말고 볼륨이나 외부 저장소로 분리합니다.

## 명령의 관계

```text
docker pull -> 이미지 준비
docker create -> 중지 상태 컨테이너 생성
docker start -> 기존 컨테이너의 명령 실행
docker run -> 필요하면 pull + create + start
docker rm -> 컨테이너와 writable layer 삭제
docker image rm -> 이미지 삭제
```

```bash
docker pull nginx:alpine
docker create --name web -p 127.0.0.1:8080:80 nginx:alpine
docker start web
docker ps
docker stop web
docker rm web
```

## 이미지 레이어 확인

Dockerfile의 각 빌드 단계는 재사용 가능한 이미지 레이어를 만들 수 있습니다. 여러 이미지가 같은 베이스 레이어를 공유하므로 디스크 사용량의 단순 합계와 실제 사용량이 다를 수 있습니다.

```bash
docker image inspect nginx:alpine
docker history nginx:alpine
docker system df
```

컨테이너에서 변경된 경로는 다음처럼 확인합니다.

```bash
docker run --name layer-demo alpine sh -c 'echo hello > /message'
docker diff layer-demo
docker rm layer-demo
```

`/message`는 컨테이너 writable layer에 있으므로 컨테이너 삭제 후 남지 않습니다.

## 태그와 digest

`nginx:alpine`에서 `alpine`은 사람이 읽기 좋은 태그이며 registry에서 다른 이미지 digest를 가리키도록 변경될 수 있습니다.

```bash
docker image inspect nginx:alpine --format '{{index .RepoDigests 0}}'
```

- 학습과 로컬 개발에서는 태그가 편리합니다.
- 재현 가능한 배포에서는 검증한 버전 태그와 digest 고정을 함께 검토합니다.
- digest를 고정하면 자동 보안 업데이트도 따라오지 않으므로 갱신 절차가 필요합니다.

## 확인 질문

1. 컨테이너 정지와 삭제는 데이터 수명에 어떤 차이가 있나요?
2. 같은 이미지에서 만든 두 컨테이너의 `/tmp` 파일이 자동 공유되지 않는 이유는 무엇인가요?
3. 운영 배포에서 이동 가능한 태그만 사용할 때 어떤 재현성 문제가 생길 수 있나요?

## 다음 문서

- [Dockerfile](02-dockerfile.md)
- [볼륨과 바인드 마운트](03-volume-bind-mount.md)

## 참고한 공식 문서

- [Docker objects](https://docs.docker.com/get-started/docker-overview/#docker-objects)
- [docker image history](https://docs.docker.com/reference/cli/docker/image/history/)

# 볼륨과 바인드 마운트

## 핵심 개념

- 볼륨은 Docker가 관리하는 영속 데이터 저장 공간입니다.
- 바인드 마운트는 호스트의 특정 경로를 컨테이너 안에 연결하는 방식입니다.
- 컨테이너가 삭제되어도 필요한 데이터를 남기려면 볼륨을 사용합니다.

## 자주 쓰는 명령어

```bash
docker volume ls
docker volume create app-data
docker run -v app-data:/data alpine
docker run -v "$PWD":/workspace alpine
docker volume inspect app-data
docker volume rm app-data
```

## 정리할 것

- 볼륨과 바인드 마운트의 차이
- 컨테이너 삭제 후 데이터가 남는 경우와 사라지는 경우
- 권한 문제 발생 시 확인할 지점

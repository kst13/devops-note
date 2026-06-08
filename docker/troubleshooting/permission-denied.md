# Permission denied

## 증상

컨테이너 안에서 파일을 읽거나 쓸 때 `Permission denied`가 발생합니다.

## 자주 보는 원인

- 바인드 마운트한 호스트 디렉터리 권한이 컨테이너 사용자와 맞지 않음
- 컨테이너가 root가 아닌 사용자로 실행됨
- 생성된 파일의 소유자가 예상과 다름

## 확인 방법

```bash
docker exec -it <container> sh
id
ls -la <path>
```

## 해결 방법

- 호스트 디렉터리 권한과 소유자를 확인합니다.
- Dockerfile의 `USER` 설정을 확인합니다.
- 필요한 경우 볼륨을 사용해 권한 충돌을 줄입니다.

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
stat -c '%u:%g %a %n' <path>
docker inspect <container> --format '{{json .Mounts}}'
```

## 해결 방법

- 호스트 디렉터리 권한과 소유자를 확인합니다.
- Dockerfile의 `USER` 설정을 확인합니다.
- 필요한 경우 볼륨을 사용해 권한 충돌을 줄입니다.
- 호스트와 컨테이너의 숫자 UID/GID를 맞추거나 시작 시 필요한 디렉터리만 소유권을 조정합니다.
- SELinux 환경의 bind mount라면 공유 범위에 맞는 `:z` 또는 `:Z` label이 필요한지 확인합니다.

`chmod 777`이나 컨테이너를 항상 root로 실행하는 방식은 권한 범위를 과도하게 넓힐 수 있으므로 원인 확인 없이 적용하지 않습니다.

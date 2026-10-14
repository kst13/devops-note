# web-portal

nginx 정적 페이지. `/api/`는 같은 네임스페이스의 `was-order` Service로 프록시한다. Jenkins가 이미지와 Helm 차트를 Nexus에 올리고, 배포는 Rancher Apps에서 버전을 골라 실행한다.

```text
Dockerfile, nginx.conf, index.html   이미지
chart/                               Helm 차트 (Deployment, Service, Ingress). 차트 버전 = 이미지 태그
Jenkinsfile                          이미지 build·push → 차트 package·push. 배포는 하지 않음
```

## Jenkins Job 설정

- Pipeline → Definition: *Pipeline script from SCM*, Script Path `Jenkinsfile`
- Credentials: `nexus` (Username with password)
- Jenkinsfile 상단 `NEXUS_DOCKER`, `NEXUS_HELM`을 사내 Nexus 주소로 수정
- 버전은 `1.0.<빌드번호>`. 이미지 태그와 차트 버전이 같다

## 로컬 확인

```bash
docker build --build-arg APP_VERSION=dev -t web-portal:dev .
docker run --rm -p 8080:80 web-portal:dev        # http://localhost:8080  (/api 는 WAS 가 없어 502)
helm lint chart && helm template web-portal chart --set image.tag=dev
```

## 배포 (Rancher)

Apps → Charts → `web-portal` → 버전 선택 → Install/Upgrade. `was-order`와 같은 네임스페이스에 설치한다. nginx가 Service 이름 `was-order`로 프록시하기 때문이다.

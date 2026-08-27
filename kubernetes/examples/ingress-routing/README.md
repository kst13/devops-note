# 도메인 기반 Ingress 라우팅 예제

`web-a`, `web-b`를 각각 컨테이너 이미지로 만들고, Traefik Ingress가 도메인에 따라
서로 다른 Service로 전달하는 예제입니다. 각 web 이미지의 Nginx는 `/api/` prefix를
제거한 뒤 Kubernetes 내부의 backend Service로 다시 프록시합니다.

```text
a.example.test → web-a Service → web-a Pod
b.example.test → web-b Service → web-b Pod
web-a `/api/<path>` → backend-a Service → backend-a Pod
web-b `/api/<path>` → backend-b Service → backend-b Pod
```

## 이미지 빌드

Docker가 실행 중인 상태에서 예제 디렉터리에서 실행합니다.

```bash
docker build -t web-a:1.0 ./web-a
docker build -t web-b:1.0 ./web-b
```

로컬 k3d에서는 호스트 Docker 이미지가 노드의 containerd에 자동으로 보이지 않으므로
이미지를 클러스터로 가져옵니다.

```bash
k3d image import web-a:1.0 web-b:1.0 -c lab
```

RKE2에서는 이미지를 사설 레지스트리에 push한 뒤 `k8s.yaml`의 `image` 값을 레지스트리
주소로 변경합니다. 운영 노드의 containerd가 레지스트리에서 이미지를 가져와 실행합니다.

## Kubernetes 리소스 배포

```bash
kubectl apply -f k8s.yaml
kubectl get pods,svc,ingress -n demo
```

## 로컬 도메인 설정

`/etc/hosts`에 다음을 추가합니다.

```text
127.0.0.1 a.example.test
127.0.0.1 b.example.test
```

현재 k3d가 호스트의 8080 포트를 loadbalancer의 80번 포트에 매핑했다면 다음처럼
확인합니다.

```bash
curl http://a.example.test:8080/
curl http://b.example.test:8080/

# web-a/web-b의 Nginx가 /api/를 제거하고 backend로 전달하는지 확인
curl http://a.example.test:8080/api/orders/42
kubectl logs -n demo deployment/backend-a
```

각 응답에 `web-a` 또는 `web-b`가 표시되면 도메인 라우팅이 성공한 것입니다. backend-a
로그에서 `/orders/42`가 보이면 원래 요청의 `/api/`가 제거된 것입니다.

## 정리

```bash
kubectl delete -f k8s.yaml
```

이 명령은 `demo` Namespace의 예제 리소스만 삭제합니다. k3d 클러스터 자체와
local-path 저장소는 삭제하지 않습니다.

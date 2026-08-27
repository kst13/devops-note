# k3d 실습 매뉴얼

이 문서는 Docker Desktop 위에 k3d 클러스터를 만들고, Traefik으로 도메인별 애플리케이션을
연결한 뒤 Rancher를 설치하는 전체 흐름을 정리합니다. 대상은 로컬 개발·학습 환경이며,
운영 서버용 k3s/RKE2 절차는 포함하지 않습니다.

## 구성

```text
호스트 PC
  ├─ 8080 → k3d serverlb:80  → Traefik → web-a/web-b
  ├─ 8443 → k3d serverlb:443 → Traefik → Rancher
  └─ 61118 → k3d serverlb:6443 → Kubernetes API

k3d-lab
  ├─ server-0
  ├─ agent-0
  ├─ agent-1
  ├─ serverlb
  └─ tools (버전·작업에 따라 보일 수 있는 보조 컨테이너)
```

## 1. 사전 준비

Docker Desktop을 실행한 뒤 다음 명령으로 확인합니다.

```bash
docker version
```

macOS + Homebrew라면 도구를 설치합니다.

```bash
brew install k3d kubectl helm
k3d version
kubectl version --client
helm version
```

## 2. 클러스터 생성

`61118`, `8080`, `8443` 포트가 사용 중인지 먼저 확인합니다.

```bash
lsof -nP -iTCP:61118 -sTCP:LISTEN
lsof -nP -iTCP:8080 -sTCP:LISTEN
lsof -nP -iTCP:8443 -sTCP:LISTEN
```

사용 중이지 않다면 다음 명령으로 server 1개, agent 2개, loadbalancer 1개를 생성합니다.

```bash
k3d cluster create lab \
  --servers 1 \
  --agents 2 \
  --api-port 127.0.0.1:61118 \
  -p "8080:80@loadbalancer" \
  -p "8443:443@loadbalancer"
```

API 포트는 로컬 PC에서만 사용하도록 `127.0.0.1`에 바인딩합니다. 다른 PC에서 API에
접속해야 하는 경우에는 사설 네트워크·방화벽·VPN을 별도로 설계해야 합니다.

생성 직후 상태를 확인합니다.

```bash
kubectl config use-context k3d-lab
kubectl get nodes -o wide
kubectl get pods -A
docker ps --filter name=k3d-lab
docker port k3d-lab-serverlb
```

## 3. Traefik 확인

k3d의 기반인 k3s는 기본적으로 Traefik을 설치합니다.

```bash
kubectl get deployment traefik -n kube-system
kubectl get service traefik -n kube-system
kubectl get pods -n kube-system -l app.kubernetes.io/name=traefik -o wide
```

Traefik은 `kube-system` Namespace의 Pod이고, `serverlb`는 Docker 레벨의 포트 전달
컨테이너입니다. 둘은 같은 구성 요소가 아닙니다.

## 4. 예제 애플리케이션 배포

예제 디렉터리로 이동합니다.

```bash
cd kubernetes/examples/ingress-routing
```

web-a와 web-b 이미지를 빌드합니다. 각 이미지에는 정적 페이지와 `/api/` reverse proxy
설정이 포함되어 있습니다.

```bash
docker build -t web-a:1.0 ./web-a
docker build -t web-b:1.0 ./web-b
```

호스트 Docker 이미지를 k3d 노드의 containerd로 가져옵니다.

```bash
k3d image import web-a:1.0 -c lab
k3d image import web-b:1.0 -c lab
```

Kubernetes 리소스를 배포합니다.

```bash
kubectl apply -f k8s.yaml
kubectl rollout status deployment --all -n demo --timeout=180s
kubectl get pods,service,ingress -n demo -o wide
```

이 파일은 다음 리소스를 생성합니다.

```text
web-a Deployment/Service       → backend-a Service
web-b Deployment/Service       → backend-b Service
web-routing Ingress
```

## 5. 도메인 라우팅 테스트

로컬 도메인을 `/etc/hosts`에 추가합니다.

```text
127.0.0.1 a.example.test
127.0.0.1 b.example.test
```

Host 헤더를 명시하면 DNS 설정과 관계없이 테스트할 수 있습니다.

```bash
curl -H 'Host: a.example.test' http://127.0.0.1:8080/
curl -H 'Host: b.example.test' http://127.0.0.1:8080/
```

첫 번째 요청은 `web-a`, 두 번째 요청은 `web-b` 응답을 반환해야 합니다.

## 6. `/api/` prefix 제거 테스트

각 web 컨테이너의 Nginx는 `/api/`를 제거한 뒤 backend Service로 전달합니다.

```bash
curl -H 'Host: a.example.test' \
  http://127.0.0.1:8080/api/orders/7

curl -H 'Host: b.example.test' \
  http://127.0.0.1:8080/api/orders/7
```

요청 변환은 다음과 같습니다.

```text
/api/orders/7
  → web-a Nginx
  → /api/ 제거
  → backend-a의 /orders/7
```

backend 로그로 실제 경로를 확인합니다.

```bash
kubectl logs -n demo deployment/backend-a --tail=20
kubectl logs -n demo deployment/backend-b --tail=20
```

로그에 `/orders/7`이 보이면 prefix 제거가 정상입니다.

## 7. Rancher 설치(선택)

Rancher는 현재 `k3d-lab`을 관리 클러스터로 사용합니다. Rancher 설치 전 HTTPS 포트가
다음처럼 연결되어 있어야 합니다.

```bash
docker port k3d-lab-serverlb
```

```text
443/tcp -> 0.0.0.0:8443
```

cert-manager를 설치합니다.

```bash
helm repo add jetstack https://charts.jetstack.io
helm repo update

helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true
```

상태를 확인합니다.

```bash
kubectl get pods -n cert-manager
```

Rancher를 설치합니다.

```bash
helm repo add rancher-stable https://releases.rancher.com/server-charts/stable
helm repo update

helm upgrade --install rancher rancher-stable/rancher \
  --namespace cattle-system \
  --create-namespace \
  --set hostname=rancher.localhost \
  --set replicas=1 \
  --set ingress.tls.source=rancher \
  --set bootstrapPassword='ChangeMe-Strong-Password-123!'
```

설치 상태를 확인합니다.

```bash
kubectl rollout status deployment/rancher \
  -n cattle-system \
  --timeout=300s
kubectl get pods,service,ingress -n cattle-system
```

브라우저에서 다음 주소로 접속합니다.

```text
https://rancher.localhost:8443
```

자체 서명 인증서 경고가 표시되면 개발 환경에 한해 예외를 허용합니다. 최초 로그인 계정은
`admin`이며 비밀번호는 설치 시 지정한 `bootstrapPassword`입니다.

## 8. Rancher HTTPS 문제 확인

TCP 연결은 되지만 TLS handshake가 실패하면 먼저 포트 대상이 `443/tcp`인지 확인합니다.

```bash
docker port k3d-lab-serverlb
```

잘못된 예:

```text
8443/tcp -> 0.0.0.0:8443
```

기존 클러스터의 매핑을 수정해야 한다면 serverlb를 직접 지정합니다.

```bash
k3d node edit k3d-lab-serverlb --port-delete 8443:8443
k3d node edit k3d-lab-serverlb --port-add 8443:443
```

올바른 예:

```text
443/tcp -> 0.0.0.0:8443
```

그 다음 TLS 요청을 확인합니다.

```bash
curl -vk \
  --resolve rancher.localhost:8443:127.0.0.1 \
  https://rancher.localhost:8443/
```

## 9. 문제 확인 순서

```bash
# 노드와 시스템 Pod
kubectl get nodes -o wide
kubectl get pods -A

# 애플리케이션 리소스
kubectl get pods,service,endpointslice,ingress -n demo

# Rancher 리소스
kubectl get pods,service,ingress -n cattle-system

# Traefik 로그
kubectl logs -n kube-system deployment/traefik --tail=100
```

일반적인 증상별 확인 대상:

| 증상 | 우선 확인할 대상 |
| --- | --- |
| 연결 거부 | Docker 포트 매핑, serverlb 상태 |
| Traefik 404 | Host 헤더, Ingress host 규칙 |
| Traefik 503 | Service selector, EndpointSlice, Pod readiness |
| TLS handshake 실패 | `443/tcp` 매핑, 인증서/Ingress 상태 |
| 이미지 Pull 실패 | `k3d image import`, 이미지 태그, `imagePullPolicy` |

## 10. 정리

애플리케이션 리소스만 삭제합니다.

```bash
kubectl delete -f k8s.yaml
```

Rancher와 cert-manager만 삭제합니다.

```bash
helm uninstall rancher -n cattle-system
helm uninstall cert-manager -n cert-manager
kubectl delete namespace cattle-system cert-manager
```

클러스터 전체를 삭제하면 Pod, Service, Ingress와 k3d 내부 local-path 데이터도 삭제될 수
있습니다. 보존할 데이터가 없는 경우에만 실행합니다.

```bash
k3d cluster delete lab
```

# Pod, Deployment, Service

쿠버네티스 워크로드의 뼈대인 세 오브젝트를 k3d 클러스터에서 직접 실행하며 익힙니다. Pod는 실행 단위, Deployment는 원하는 상태 유지, Service는 고정된 접근 지점입니다. 여기서 작성하는 YAML은 k3s, kind, EKS 어디서든 그대로 동작합니다.

## 실습 준비

Docker가 설치되어 있다면 k3d로 수 초 만에 클러스터를 만들 수 있습니다.

```bash
brew install k3d kubectl   # macOS 기준. 다른 OS는 k3d 공식 설치 스크립트 사용
k3d cluster create devops-note
kubectl get nodes
```

`kubectl get nodes`에서 STATUS가 Ready인 노드 1개가 보이면 준비 완료입니다. k3d가 kubeconfig를 자동으로 등록해 주므로 kubectl이 바로 이 클러스터를 가리킵니다.

## Pod: 최소 실행 단위

Pod는 쿠버네티스가 실행하는 최소 단위입니다. 보통 컨테이너 1개를 담고, 필요하면 네트워크와 스토리지를 공유하는 보조 컨테이너를 함께 담습니다. 컨테이너가 아니라 Pod가 단위인 이유는, 반드시 같은 노드에서 함께 떠야 하는 컨테이너 묶음을 표현하기 위해서입니다.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: web-pod
spec:
  containers:
    - name: web
      image: nginx:1.27-alpine
      ports:
        - containerPort: 80
```

```bash
kubectl apply -f pod.yaml
kubectl get pods
kubectl delete pod web-pod
```

실무에서 Pod를 이렇게 직접 만드는 일은 거의 없습니다. 단독 Pod는 죽어도 되살아나지 않기 때문입니다. 방금 `delete`로 지운 Pod가 다시 생기지 않는 것이 그 증거입니다. "죽으면 되살리는" 책임은 다음 오브젝트가 집니다.

## Deployment: 원하는 상태 유지

Deployment는 "이 Pod가 몇 개, 어떤 버전으로 떠 있어야 한다"는 선언입니다. 컨트롤러가 이 선언과 현재 상태를 계속 비교하면서 수렴시킵니다.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 2
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: web
          image: nginx:1.27-alpine
          ports:
            - containerPort: 80
```

`selector.matchLabels`와 `template.metadata.labels`가 같아야 합니다. Deployment는 이 라벨로 "내가 관리하는 Pod"를 식별하기 때문입니다.

```bash
kubectl apply -f deployment.yaml
kubectl get pods                          # web-으로 시작하는 Pod 2개
```

선언적 모델이 실제로 동작하는지 확인해 봅니다. Pod 하나를 강제로 지우면 몇 초 안에 새 Pod가 자동으로 생깁니다.

```bash
kubectl delete pod $(kubectl get pods -l app=web -o name | head -1)
kubectl get pods                          # 여전히 2개 (하나는 AGE가 어림)
```

스케일과 롤링 업데이트도 선언을 바꾸는 일일 뿐입니다.

```bash
kubectl scale deployment web --replicas=4
kubectl set image deployment/web web=nginx:1.28-alpine
kubectl rollout status deployment/web     # 무중단으로 하나씩 교체되는 과정 관찰
kubectl rollout undo deployment/web       # 문제가 있으면 이전 버전으로 롤백
```

`scale`과 `set image`는 학습용으로는 편하지만, 실무에서는 YAML 파일을 고쳐 `kubectl apply` 하는(나아가 Git으로 관리하는) 방식이 원칙입니다. 명령으로 바꾼 상태는 파일에 남지 않아 다음 apply 때 되돌아가기 때문입니다.

## Service: 고정된 접근 지점

Pod는 죽고 다시 생길 때마다 IP가 바뀝니다. 방금 실습에서도 Pod가 교체될 때마다 새 IP를 받았습니다. 그래서 Pod IP로 직접 통신하는 설계는 성립하지 않고, 고정된 이름과 주소를 제공하는 Service를 사이에 둡니다.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  selector:
    app: web
  ports:
    - port: 80
      targetPort: 80
```

Service는 `selector`와 일치하는 라벨을 가진 Pod들을 찾아 트래픽을 분산합니다. Pod가 교체되어도 라벨만 같으면 자동으로 새 Pod를 대상에 포함합니다.

```bash
kubectl apply -f service.yaml
kubectl get svc web
kubectl port-forward svc/web 8080:80      # 로컬 8080 → Service 80
curl http://localhost:8080                # nginx 기본 페이지 응답
```

클러스터 안의 다른 Pod에서는 `http://web` 이라는 DNS 이름만으로 이 Service에 접근합니다. 설정 파일에 Pod IP 대신 Service 이름을 적는 것이 쿠버네티스 내부 통신의 기본형입니다.

Service에는 노출 범위에 따라 세 타입이 있습니다.

| 타입 | 노출 범위 | 비고 |
| --- | --- | --- |
| ClusterIP | 클러스터 내부만 (기본값) | 내부 서비스 간 통신 |
| NodePort | 각 노드의 고정 포트(30000-32767)로 외부 노출 | 간단하지만 포트 관리가 번거로움 |
| LoadBalancer | 외부 로드밸런서를 통해 노출 | 구현이 환경마다 다름 — 아래 참고 |

LoadBalancer 타입은 "가장자리"의 대표적인 예입니다. k3d/k3s에서는 내장 ServiceLB가, EKS에서는 실제 AWS 로드밸런서가 만들어지고, kind에서는 기본적으로 아무 일도 일어나지 않아 pending 상태로 남습니다. 워크로드 YAML은 어디서나 같지만 외부 노출은 환경에 의존한다는 것 — 이 구분은 [쿠버네티스를 경험하는 방법들](02-ways-to-run-kubernetes.md)의 환경 전략과 연결됩니다.

## 정리

실습 리소스를 지우거나 클러스터째 제거합니다.

```bash
kubectl delete deployment web
kubectl delete service web
k3d cluster delete devops-note   # 클러스터째 지울 때
```

Pod(실행 단위) → Deployment(상태 유지) → Service(접근 지점)의 관계가 쿠버네티스 워크로드의 기본형입니다. 다음 문서 [설정과 볼륨](04-config-and-volume.md)에서 이 위에 설정과 데이터를 얹습니다.

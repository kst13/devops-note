# 쿠버네티스 기본기

쿠버네티스가 어떤 문제를 풀기 위해 존재하는지, 클러스터가 어떤 부품으로 구성되는지, "선언적 모델"이 무엇을 의미하는지 정리합니다. 이 문서의 내용은 k3s, kind, EKS 등 어떤 쿠버네티스를 쓰든 동일하게 적용됩니다.

## 왜 쿠버네티스인가

[Docker와 Compose](../../docker/concepts/05-compose.md)만으로도 서버 한 대에서 컨테이너 여러 개를 운영할 수 있습니다. 문제는 서버가 여러 대가 되는 순간부터입니다.

- 컨테이너를 어느 서버에 배치할지 누가 결정하는가
- 서버 한 대가 죽으면 그 위의 컨테이너를 누가 다른 서버에 되살리는가
- 새 버전을 배포할 때 서비스 중단 없이 컨테이너를 교체하려면 어떻게 하는가
- 트래픽이 늘면 컨테이너 수를 누가 늘리고, 늘어난 컨테이너를 어떻게 찾아가는가

이 일들을 사람이 서버마다 접속해서 하는 대신, 클러스터 전체를 하나의 컴퓨터처럼 다루며 자동화하는 것이 쿠버네티스의 역할입니다. 반대로 말하면 서버 한 대, 소수의 컨테이너 규모에서는 Compose가 더 단순하고 적합할 수 있습니다. 도구가 아니라 문제의 크기가 선택을 결정합니다.

## 클러스터 구조

쿠버네티스 클러스터는 결정을 내리는 Control Plane과 컨테이너를 실제로 실행하는 Node로 나뉩니다.

| 구성 요소 | 위치 | 역할 |
| --- | --- | --- |
| kube-apiserver | Control Plane | 모든 조작의 유일한 창구. kubectl도, 내부 컴포넌트도 전부 API 서버를 통해 통신 |
| etcd | Control Plane | 클러스터의 모든 상태를 저장하는 키-값 저장소. 이것을 잃으면 클러스터를 잃는 것 |
| scheduler | Control Plane | 새 Pod를 어느 Node에 배치할지 결정 |
| controller-manager | Control Plane | "원하는 상태"와 "현재 상태"를 비교해 수렴시키는 컨트롤러들의 집합 |
| kubelet | Node | 각 Node의 에이전트. API 서버의 지시대로 컨테이너를 실행하고 상태를 보고 |
| 컨테이너 런타임 | Node | 컨테이너를 실제로 실행하는 엔진 (containerd 등) |
| kube-proxy | Node | Service의 트래픽을 Pod로 전달하는 네트워크 규칙 관리 |

기억할 것은 하나입니다. 모든 조작은 API 서버를 거칩니다. kubectl 명령도, 대시보드 클릭도, 컨트롤러의 동작도 전부 API 서버에 대한 요청이며, 그 결과 상태는 etcd에 기록됩니다. 그래서 API 서버의 주소와 인증 정보(kubeconfig)만 있으면 어디서든 클러스터를 조작할 수 있습니다.

## 선언적 모델

쿠버네티스를 처음 쓸 때 가장 중요한 사고 전환입니다. "컨테이너를 실행해라"라고 명령하는 것이 아니라, "이런 상태이기를 원한다"를 선언합니다.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 2          # web 컨테이너가 항상 2개 떠 있기를 원한다
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
```

```bash
kubectl apply -f deployment.yaml
```

이 선언을 적용하면 컨트롤러가 현재 상태를 감시하면서 선언과 다르면 스스로 수렴시킵니다. Pod 하나가 죽으면 명령 없이도 새로 만들어 2개를 유지하고, replicas를 4로 고쳐 다시 적용하면 2개를 더 띄웁니다. 장애 복구, 스케일, 배포가 전부 "선언을 바꾸는 일"로 통일되는 것이 이 모델의 힘이고, 뒤에서 다룰 GitOps(선언 파일을 Git으로 관리) 같은 운영 방식도 여기서 출발합니다.

## 핵심 오브젝트 한눈에

| 오브젝트 | 역할 | 자세한 문서 |
| --- | --- | --- |
| Pod | 컨테이너를 실행하는 최소 단위 | [Pod, Deployment, Service](03-pod-deployment-service.md) |
| Deployment | Pod 묶음의 개수·버전을 원하는 상태로 유지 | [Pod, Deployment, Service](03-pod-deployment-service.md) |
| Service | 계속 바뀌는 Pod들에 대한 고정된 접근 지점 | [Pod, Deployment, Service](03-pod-deployment-service.md) |
| ConfigMap / Secret | 설정과 비밀 값을 이미지 밖으로 분리 | [설정과 볼륨](04-config-and-volume.md) |
| Namespace | 오브젝트들을 묶는 논리적 구획 (팀·환경별 분리) | 입문 실습에서는 기본값 default 사용 |

## k3s는 무엇이 다른가

이 시리즈의 실서버 기준인 k3s는 위 컴포넌트들을 전부 단일 바이너리 하나에 담은 경량 배포판입니다. 표준 구성과의 차이는 다음과 같습니다.

- etcd 대신 sqlite가 기본 저장소입니다 (HA 구성 시 내장 etcd 선택 가능).
- Ingress(Traefik), LoadBalancer(ServiceLB), 스토리지(local-path)가 기본 내장되어 설치 직후 바로 씁니다.
- 오래된 기능과 클라우드 제공자 연동 코드를 덜어내 바이너리가 약 70MB로 작습니다.

중요한 것은 k3s가 CNCF 인증을 통과한 정식 쿠버네티스라는 점입니다. API가 동일하므로 여기서 배우는 개념과 YAML은 모든 쿠버네티스에 그대로 통하고, 차이는 "무엇이 미리 포함되어 있는가"뿐입니다. 실행 환경별 차이는 [쿠버네티스를 경험하는 방법들](02-ways-to-run-kubernetes.md)에서 비교합니다.

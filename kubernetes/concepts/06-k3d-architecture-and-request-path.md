# k3d 구조와 요청 경로

k3d가 Docker 위에 만들어 주는 것이 정확히 무엇인지, kubectl 명령과 브라우저 요청이 어떤 경로로 클러스터에 닿는지를 Docker 사용자의 눈으로 해부합니다. [워크로드 실습](03-pod-deployment-service.md)의 단일 노드 클러스터를 멀티 노드로 확장하고, Docker 경험이 쿠버네티스의 어느 개념에 대응하는지 정리합니다.

## 멀티 노드 클러스터 만들기

```bash
k3d cluster create lab --agents 2 -p "8080:80@loadbalancer"
```

옵션의 의미가 중요합니다.

- `--agents 2` — 워커(agent) 노드 2개. server는 지정하지 않아도 기본 1개이므로 총 3노드가 됩니다.
- `-p "8080:80@loadbalancer"` — 로컬 8080 포트를 클러스터 진입점(로드밸런서 컨테이너)의 80으로 매핑합니다. `docker run -p 8080:80`과 같은 문법에 "어느 컨테이너에 걸지"(`@loadbalancer`)만 추가된 형태입니다.

### 다른 PC에서 같은 구조 재현

Docker Desktop이 실행 중인 PC에 k3d와 kubectl을 설치한 뒤 같은 명령을 실행하면 됩니다.

```bash
# macOS + Homebrew 예시
brew install k3d kubectl

# server 1개, agent 2개, loadbalancer 1개
k3d cluster create lab \
  --servers 1 \
  --agents 2 \
  --api-port 0.0.0.0:61118 \
  -p "8080:80@loadbalancer"
```

`61118`은 호스트 PC에서 사용하지 않는 포트로 바꿀 수 있습니다. API 포트를 생략하면
k3d가 임의의 포트를 선택합니다. 클러스터 생성 후 kubeconfig에 `k3d-lab` 컨텍스트가
등록되는지 확인합니다.

```bash
kubectl config current-context
kubectl get nodes -o wide
docker ps --filter name=k3d-lab
```

다른 PC에서 실행하면 같은 토폴로지의 **새 클러스터**가 만들어질 뿐, 기존 PC의 Pod·PVC·
데이터가 복사되는 것은 아닙니다. 애플리케이션을 재현하려면 YAML을 별도로 복사해
`kubectl apply -f`로 적용해야 합니다.

포트 매핑은 생성 시점에 걸어야 합니다. 이것이 없으면 나중에 Ingress를 만들어도 브라우저에서 접속할 방법이 없어, k3d 입문에서 가장 자주 막히는 지점이 됩니다.

워커를 굳이 2개로 만드는 이유는 학습 효과 때문입니다. 워커가 1개면 모든 Pod가 무조건 거기에 뜨므로 "클러스터가 노드를 골라 배치한다"는 스케줄링을 관찰할 수 없고, `kubectl drain`으로 노드를 비웠을 때 Pod가 다른 노드로 옮겨 가는 것도 볼 수 없습니다. 여러 대 위에 분산되는 것을 체감할 수 있는 최소 구성이 server 1 + agent 2입니다.

생성 직후에는 정상 기동을 확인합니다.

```bash
kubectl get nodes -o wide      # 3개 노드 모두 Ready
kubectl get pods -A            # 시스템 Pod 상태

# 시스템 컴포넌트(coredns, traefik 등)가 다 뜰 때까지 대기
kubectl wait --for=condition=available deployment --all -n kube-system --timeout=180s
```

클러스터는 생성 명령이 끝난 순간 완성되는 것이 아니라, 내부에서 CoreDNS·Traefik·local-path-provisioner 같은 시스템 Pod가 순차적으로 올라옵니다. "만들었다"와 "다 떴다"를 구분해 확인하는 습관이 실서버에서도 그대로 쓰입니다.

## k3d 컨테이너의 정체

생성 후 `docker ps`를 보면 기본적으로 server, agent, serverlb 컨테이너가 보이고, k3d
버전이나 이미지 import 작업에 따라 `tools` 보조 컨테이너도 보일 수 있습니다.

```text
NAMES              IMAGE                            PORTS
k3d-lab-serverlb   ghcr.io/k3d-io/k3d-proxy        0.0.0.0:8080->80/tcp, 0.0.0.0:<API 포트>->6443/tcp
k3d-lab-agent-1    rancher/k3s
k3d-lab-agent-0    rancher/k3s
k3d-lab-server-0   rancher/k3s
k3d-lab-tools      ghcr.io/k3d-io/k3d-tools
```

`rancher/k3s` 이미지로 뜬 3개는 nginx처럼 "프로그램 하나를 담은 컨테이너"가 아니라 **리눅스 서버 1대인 척하는 컨테이너**입니다. 실서버라면 머신 3대에 각각 k3s를 설치했을 것을, 로컬에서는 컨테이너 3개로 흉내 낸 것입니다. 여기에 serverlb, 포트 매핑, kubeconfig 관리 등을 더해 로컬에서 k3s 클러스터를 빠르게 만들고 지울 수 있게 한 도구가 k3d입니다.

| 컨테이너 | 역할 |
| --- | --- |
| server-0 | Control Plane. API 서버, 스케줄러, 상태 저장소가 여기서 동작 |
| agent-0, agent-1 | 워커. 실제 워크로드(Pod)가 뜨는 곳 |
| serverlb | 기본 구성에서 API와 Ingress 트래픽이 들어오는 진입점 (아래 요청 경로 참고) |
| tools | k3d 관리용 보조 컨테이너. 이미지 import 등에서 사용되며 Kubernetes 노드는 아님 |

## 컨테이너 안의 컨테이너

Docker 경험과 충돌하는 지점이 하나 있습니다. 노드 역할을 하는 컨테이너 안에는 **자체 컨테이너 런타임(containerd)이 또 들어 있습니다**. 워크로드를 배포하면 그 컨테이너는 agent 컨테이너 내부의 containerd가 실행하므로, 호스트의 `docker ps`에는 절대 보이지 않습니다.

```bash
kubectl create deployment web --image=nginx --replicas=2
docker ps | grep nginx         # 아무것도 안 나옴
kubectl get pods -o wide       # 여기에 나옴 — 어느 노드에 배치됐는지도 함께
```

클러스터 안의 컨테이너는 Docker CLI가 아니라 kubectl로 본다는 것, 그리고 `-o wide`로 배치된 노드까지 확인하는 것이 관찰의 기본형입니다.

## Docker 개념과의 대응

Docker에서 `docker run -d -p 8080:80 nginx` 한 줄이 하던 일을, 쿠버네티스는 역할별로 쪼갭니다. 서버가 여러 대이고, 컨테이너가 몇 개로 늘어날지 모르고, 어디서 죽을지 모르는 환경을 전제하기 때문입니다.

| Docker에서 하던 것 | 쿠버네티스에서 | 왜 바뀌나 |
| --- | --- | --- |
| `docker run -d nginx` | Deployment (→ Pod 생성) | "실행해라"가 아니라 "N개가 항상 떠 있는 상태를 유지해라"는 선언. 죽으면 재생성, 배치 노드는 클러스터가 결정 |
| 컨테이너 이름으로 통신 | Service | Pod는 재생성될 때마다 IP가 바뀌므로 고정된 이름의 접점이 트래픽을 분배 |
| `docker run -p 8080:80` | Ingress | 노드가 여러 대라 특정 노드에 포트를 묶을 수 없음. "어떤 요청을 어느 Service로 보낼지"의 라우팅 규칙 |

각 오브젝트의 동작은 [Pod, Deployment, Service](03-pod-deployment-service.md)에서 실습합니다.

## 요청 경로 두 가지

serverlb의 포트 매핑을 다시 보면 문이 2개입니다.

| 포트 매핑 | 용도 | 쓰는 사람 |
| --- | --- | --- |
| `<API 포트> → 6443` | 관리 트래픽 (kubectl → API 서버) | 운영자 |
| `8080 → 80` | 앱 트래픽 (브라우저 → Ingress → 앱) | 서비스 사용자 |

`<API 포트>`는 k3d가 생성 시 임의로 고른 호스트 포트라 클러스터를 다시 만들면 바뀔 수 있습니다. 6443은 쿠버네티스 API 서버가 serverlb 뒤에서 사용하는 표준 포트입니다.

**관리 트래픽.** kubectl은 마법이 아니라 HTTPS 요청을 보내는 REST 클라이언트이고, 어디로 보낼지를 `~/.kube/config`에서 읽습니다. k3d가 클러스터 생성 시 이 파일에 접속 주소와 관리자 인증서를 자동 등록해 둡니다.

```bash
kubectl config view --minify | grep server
# server: https://0.0.0.0:<API 포트>

# Docker에 실제로 매핑된 포트 확인
docker port k3d-lab-serverlb 6443/tcp
```

```text
kubectl (호스트)
  → https://localhost:<API 포트>      kubeconfig에 적힌 주소
  → Docker 포트 매핑 <API 포트> → 6443
  → serverlb 컨테이너 (프록시)
  → server-0의 6443 (API 서버)       인증서 확인 후 요청 처리
```

**앱 트래픽.** 첫 실습에서 만드는 리소스들이 이 경로의 조각을 안쪽부터 하나씩 채웁니다.

```text
curl localhost:8080
  → Docker 포트 매핑 8080 → 80        생성 시 -p "8080:80@loadbalancer"
  → Traefik (Ingress 컨트롤러 Pod)
  → Ingress 규칙: "/ 경로는 web으로"
  → web Service가 살아있는 Pod 선택
  → nginx Pod 응답
```

실서버에서도 구조는 같습니다. kubeconfig의 `server:` 주소가 localhost 대신 Control Plane 앞의 로드밸런서나 VIP 주소로 바뀔 뿐입니다. 실서버에서 이 고정 접점을 어떻게 만드는지는 [실서버 클러스터 토폴로지](07-production-cluster-topology.md)에서 다룹니다.

## 기동 직후 확인할 증상: helm-install Pod의 CrashLoopBackOff

- **증상**: 클러스터 생성 직후 `kubectl get pods -A`에서 `helm-install-traefik` Pod가 `CrashLoopBackOff`.
- **가능한 원인**: 초기 의존 리소스가 아직 준비되지 않은 일시적 실패일 수 있지만, 이미지 다운로드·권한·네트워크 오류도 같은 상태를 만듭니다. 상태만 보고 정상이라고 단정하지 않습니다.
- **확인**: Job의 완료 여부, Pod 상세 정보·이벤트, 로그를 함께 확인합니다. 로그 끝에 `STATUS: deployed`가 있고 Job이 `Complete`라면 설치가 성공한 것입니다.

  ```bash
  kubectl get job -n kube-system helm-install-traefik
  kubectl describe pod -n kube-system -l batch.kubernetes.io/job-name=helm-install-traefik
  kubectl get events -n kube-system --sort-by=.lastTimestamp
  kubectl logs -n kube-system job/helm-install-traefik --tail=20
  ```

- **해결**: Job이 `Complete`면 완료된 Pod가 `Completed`로 남는 것은 정상이며 추가 조치가 필요 없습니다. Job이 계속 실패하면 로그와 이벤트의 원인을 해결한 뒤 재생성하거나 클러스터를 다시 만듭니다. "상태가 이상하다 → 상세 정보·이벤트·로그로 원인 확인"이 쿠버네티스 트러블슈팅의 기본 동작입니다.

## kubectl 버전 확인 (macOS)

kubectl은 클러스터와 마이너 버전 ±1까지만 공식 지원합니다. macOS에서 Docker Desktop을 쓰고 있다면 `/usr/local/bin/kubectl`이 Docker Desktop 번들 버전(구버전인 경우가 많음)을 가리키는 심볼릭 링크일 수 있습니다.

```bash
kubectl version                # Client와 Server 버전 차이 확인
ls -la /usr/local/bin/kubectl  # Docker.app을 가리키면 번들 버전
```

버전 차이가 ±1을 넘으면 최신 kubectl을 직접 받아 PATH 우선순위가 높은 경로에 둡니다. CPU 아키텍처에 따라 다운로드 경로가 다르며, Apple Silicon은 `/opt/homebrew/bin`, Intel Mac은 `/usr/local/bin`을 주로 사용합니다.

```bash
ARCH=$(uname -m)
case "$ARCH" in
  arm64)  KUBECTL_ARCH=arm64; INSTALL_DIR=/opt/homebrew/bin ;;
  x86_64) KUBECTL_ARCH=amd64; INSTALL_DIR=/usr/local/bin ;;
  *) echo "지원하지 않는 macOS 아키텍처: $ARCH" >&2; exit 1 ;;
esac
STABLE=$(curl -L -s https://dl.k8s.io/release/stable.txt)
curl -sLO "https://dl.k8s.io/release/${STABLE}/bin/darwin/${KUBECTL_ARCH}/kubectl"
chmod +x kubectl
# Homebrew 경로 등 PATH에 포함된 디렉터리로 이동
mv kubectl "${INSTALL_DIR}/kubectl"
```

## 부수고 다시 만들기

k3d의 최대 장점은 재생성 비용이 0에 가깝다는 것입니다.

```bash
# 실습 리소스와 컨테이너 내부 local-path PV 데이터까지 삭제될 수 있음
k3d cluster delete lab
```

삭제 후 `docker ps`와 `~/.kube/config`가 어떻게 변하는지, 재생성하면 API 포트 번호가 바뀌는 것까지 관찰해 보면 이 문서의 구조가 전부 연결됩니다. k3d 기본 local-path 저장소는 노드 컨테이너의 파일시스템 안에 있으므로, 보존할 데이터가 있으면 삭제 전에 호스트 디렉터리를 볼륨으로 매핑하거나 별도로 백업합니다. 실험하다 망가뜨렸을 때 고치려 애쓰기보다 다시 만드는 편이 빠른 경우가 많고, 그 부담 없음이 학습 속도를 만듭니다.

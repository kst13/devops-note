# k3s 클러스터 설치

로컬 k3d에서 배운 것을 실서버로 옮깁니다. k3s를 리눅스 서버에 설치해 단일 노드 클러스터를 만들고, 노드를 추가해 멀티 노드로 확장하고, 외부에서 kubectl로 접속하는 절차를 다룹니다. k3d와 k3s는 같은 스택이므로 [워크로드 실습](03-pod-deployment-service.md)에서 쓴 YAML이 그대로 동작합니다.

## 준비물

- 리눅스 서버 1대 이상 (2 CPU, 2GB 메모리 이상 권장 — k3s 자체는 512MB에서도 돌지만 워크로드 여유분이 필요합니다)
- 노드 간 통신을 위한 방화벽 허용

| 포트 | 프로토콜 | 용도 |
| --- | --- | --- |
| 6443 | TCP | 쿠버네티스 API 서버 (agent → server, kubectl → server) |
| 8472 | UDP | Flannel VXLAN (노드 간 Pod 네트워크) |
| 10250 | TCP | kubelet (로그·exec 등 노드 간 통신) |

## 단일 노드 설치

k3s 설치는 공식 스크립트 한 줄입니다.

```bash
curl -sfL https://get.k3s.io | sh -
```

이 스크립트가 하는 일은 세 가지입니다. 단일 바이너리를 `/usr/local/bin/k3s`에 설치하고, systemd 서비스(`k3s.service`)로 등록해 부팅 시 자동 시작되게 하고, 관리자 kubeconfig를 `/etc/rancher/k3s/k3s.yaml`에 생성합니다. Control Plane과 워커 역할을 한 프로세스가 모두 수행하므로, 이 한 대만으로 완전한 클러스터입니다.

```bash
sudo systemctl status k3s          # active (running) 확인
sudo k3s kubectl get nodes         # Ready 노드 1개
sudo k3s kubectl get pods -A       # traefik, coredns 등 시스템 Pod 확인
```

`k3s kubectl`은 k3s에 내장된 kubectl입니다. 서버에 kubectl을 따로 설치할 필요가 없습니다.

## 멀티 노드 구성

노드를 추가하려면 server가 발급한 토큰이 필요합니다. 토큰은 조인하는 노드가 올바른 클러스터에 합류하는지 검증하는 자격 증명이므로 비밀로 다룹니다.

```bash
# server 노드에서 토큰 확인
sudo cat /var/lib/rancher/k3s/server/node-token
```

추가할 서버에서 같은 설치 스크립트에 server 주소와 토큰만 환경 변수로 넘기면 agent로 조인합니다.

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://server-1.example.internal:6443 \
  K3S_TOKEN=${K3S_TOKEN} sh -
```

```bash
# server 노드에서 확인
sudo k3s kubectl get nodes         # 새 노드가 Ready로 추가됨
```

역할 구분은 단순합니다. server는 Control Plane(API 서버, 스케줄러, 저장소)을 실행하면서 워크로드도 받고, agent는 워크로드만 실행합니다. server가 1대면 그 서버 장애 시 클러스터 제어가 멈추므로, 고가용성이 필요하면 server를 3대로 늘리고 내장 etcd를 씁니다(첫 server를 `--cluster-init` 옵션으로 시작). HA 구성의 상세 절차는 이 문서 범위 밖이며, k3s 공식 문서의 High Availability 절을 참고합니다. server를 왜 3대로 두는지, 머신을 몇 대 확보해야 하는지는 [실서버 클러스터 토폴로지](07-production-cluster-topology.md)에서 다룹니다.

## 기본 내장 컴포넌트

k3s는 설치 직후 바로 쓸 수 있도록 필수 컴포넌트를 내장합니다. 표준 구성과 다른 지점이므로 무엇이 들어 있는지 알아야 합니다.

| 컴포넌트 | 역할 | 표준 구성과의 차이 | 비활성화 |
| --- | --- | --- | --- |
| Traefik | Ingress 컨트롤러 | RKE2 등 표준 구성은 ingress-nginx가 일반적 | `--disable traefik` |
| ServiceLB | LoadBalancer 타입 Service 구현 | 클라우드는 실제 LB, kubeadm은 별도 설치 필요 | `--disable servicelb` |
| local-path-provisioner | 기본 StorageClass (노드 로컬 디스크) | 데이터가 노드에 묶임 — 멀티 노드에서 제약 | `--disable local-storage` |
| Flannel | CNI (Pod 네트워크) | 다른 CNI를 쓰려면 `--flannel-backend=none` | 위 옵션으로 교체 |

비활성화 옵션은 설치 시 `INSTALL_K3S_EXEC` 환경 변수나 `/etc/rancher/k3s/config.yaml`로 지정합니다. 예를 들어 ingress-nginx를 쓰기로 팀 표준을 정했다면 Traefik을 끄고 시작하는 편이 명확합니다. 이 "기본 내장" 목록이 곧 [환경의 가장자리](02-ways-to-run-kubernetes.md)이며, 스테이지·운영을 다른 배포판으로 가져갈 계획이라면 여기 의존한 부분을 미리 파악해 둡니다.

## 외부에서 kubectl 접속

서버에 접속해 `k3s kubectl`을 치는 대신, 작업 PC에서 직접 클러스터를 조작하는 것이 일상적인 사용 방식입니다.

```bash
# 1) server 노드의 kubeconfig를 작업 PC로 복사
scp user@server-1.example.internal:/etc/rancher/k3s/k3s.yaml ~/.kube/k3s-config

# 2) server 주소를 127.0.0.1에서 실제 호스트명으로 교체
#    (파일 안의 server: https://127.0.0.1:6443 부분)
sed -i '' 's/127.0.0.1/server-1.example.internal/' ~/.kube/k3s-config

# 3) 이 kubeconfig로 접속
export KUBECONFIG=~/.kube/k3s-config
kubectl get nodes
```

kubeconfig에는 클러스터 관리자 인증서가 통째로 들어 있습니다. 이 파일 하나면 클러스터의 모든 것을 조작할 수 있으므로, 유출되지 않게 다루고 Git에 커밋하지 않습니다.

## 제거

k3s는 제거 스크립트도 함께 설치합니다. 다만 이 스크립트는 실행 파일만 지우는 명령이
아닙니다. 실행 중인 k3s와 Pod를 중지하고, 해당 노드의 로컬 클러스터 데이터·설정·CLI
도구를 삭제합니다. 특히 기본 `local-path` StorageClass를 사용했다면 PV 데이터가 기본적으로
`/var/lib/rancher/k3s/storage` 아래에 있으므로, 서버에서 다음 명령을 실행하면 애플리케이션
데이터와 SQLite/내장 etcd 상태를 함께 잃을 수 있습니다. 외부 데이터베이스나 외부 스토리지
볼륨의 데이터까지 지우는 것은 아니지만, 이를 전제로 복구된다고 가정해서는 안 됩니다.

```bash
sudo /usr/local/bin/k3s-uninstall.sh          # server 노드
sudo /usr/local/bin/k3s-agent-uninstall.sh    # agent 노드
```

위 명령은 실습용으로 버릴 클러스터를 완전히 정리할 때만 사용합니다. 운영 데이터가 있거나
잠시 멈췄다가 다시 사용할 계획이라면 먼저 목적에 맞는 대안을 선택합니다.

### 데이터를 유지한 채 잠시 중지

서비스만 중지하면 k3s 데이터 디렉터리와 kubeconfig는 남습니다. 중지하는 동안 API 서버와
워크로드는 사용할 수 없으므로, 재개할 때 `start`를 실행합니다.

```bash
# server
sudo systemctl stop k3s
sudo systemctl start k3s   # 다시 사용할 때

# agent라면 서비스 이름만 k3s-agent로 변경
# sudo systemctl stop k3s-agent
# sudo systemctl start k3s-agent   # 다시 사용할 때
```

### 백업 후 완전 제거

완전히 제거해야 한다면 먼저 Kubernetes 오브젝트와 호스트의 로컬 데이터를 별도로 백업합니다.
백업 파일에는 kubeconfig와 시크릿이 포함될 수 있으므로 접근 권한을 제한하고, 복구 테스트가
끝날 때까지 삭제하지 않습니다.

```bash
# API가 살아 있는 동안 리소스 정의를 백업(민감한 값 포함 여부를 확인)
sudo k3s kubectl get all,configmap,secret,pvc -A -o yaml \
  > /root/k3s-resources-$(date +%F).yaml

# k3s 중지 후 로컬 datastore와 local-path PV 데이터를 보관
sudo systemctl stop k3s
sudo tar -C /var/lib/rancher -czf /root/k3s-data-$(date +%F).tgz k3s
sudo tar -C /etc/rancher -czf /root/k3s-config-$(date +%F).tgz k3s

# 백업과 복구 가능성을 확인한 뒤에만 실행
sudo /usr/local/bin/k3s-uninstall.sh
```

agent 노드는 해당 노드에서 `k3s-agent-uninstall.sh`를 실행합니다. 기존 클러스터에 다시
조인할 노드라면 제거 전에 server에서 `kubectl delete node <노드명>`으로 기존 노드 등록도
정리해야 합니다. 자세한 삭제 범위는 [K3s 공식 제거 문서](https://docs.k3s.io/kr/installation/uninstall),
local-path의 기본 저장 위치는 [K3s 공식 스토리지 문서](https://docs.k3s.io/add-ons/storage)를
확인합니다.

여기까지 오면 로컬 실습(k3d)과 실서버 운영(k3s)이 같은 스택으로 연결됩니다. 일상 조작에 필요한 명령은 [kubectl 치트시트](../commands/kubectl-cheatsheet.md)에 정리되어 있습니다.

# 실서버 Rancher + RKE2 구축 준비: 점검, 인프라 요청, 방화벽, dev 노드

사내 인프라팀이 관리하는 실서버(Rocky Linux 9)에 Rancher 관리 클러스터와 dev 클러스터를 올리기 전에 해야 할 준비를 정리합니다. [15 RKE2 3대 클러스터 구축](15-rke2-three-node-build.md)이 "내가 모든 권한을 가진 서버"를 가정한 절차라면, 이 문서는 **OS 설정은 인프라팀이 하고, 나는 제한된 sudo와 `/app` 디렉터리만 받은** 상황을 다룹니다. 설계 배경은 [16 Rancher로 여러 클러스터 중앙 관리](16-rancher-multi-cluster-management.md)에 있습니다.

## 0. 전제와 예시 주소

```text
관리자 PC ──443──▶ L4 VIP 10.0.0.10 (rancher.example.internal) ──▶ MGMT-1~3 :443
                                                                  10.0.0.11 / .12 / .13
dev 노드 DEV-1~3 (10.0.1.11 / .12 / .13) ──443──▶ 위 VIP        ← 에이전트 단방향
Jenkins ──443──▶ 위 VIP                                            ← 배포
DEV 노드 ──5000·5001──▶ Nexus                                      ← 이미지 pull
```

| 항목 | 값 | 비고 |
| --- | --- | --- |
| OS | Rocky Linux 9.8, 3대 + 3대 | hostname, 계정, 디스크는 인프라팀이 설정 완료 |
| 내 권한 | 일반 계정 + 설치 기간 sudo, `/app`(500GB) 소유 | `/etc`, `/var` 변경은 sudo |
| Rancher 접점 | 인프라팀 L4가 443을 세 노드로 분산 | kube-vip 불필요. 6443·9345는 L4를 거치지 않음 |
| dev 접점 | L4 없음. MetalLB L2 VIP `10.0.1.20` | 인프라팀에 IP 1개 예약 요청 |
| 아웃바운드 | 기본 차단 | 도메인 단위로 허용 요청 |
| 버전 | RKE2 v1.36.4+rke2r1, Rancher 2.15.2 | Rancher 2.15가 RKE2 1.34~1.36과 Rocky 9.8을 지원 |

주소는 모두 예시입니다. 실제 값으로 바꿔 읽으세요.

## 1. 서버 상태 점검

세 대에서 한 번에 실행하고 기대값과 비교합니다. sudo가 필요 없는 명령만 모았습니다.

```bash
echo "== hostname (3대 모두 달라야 함)"; hostnamectl --static
echo "== swap (관리 서버는 0 이어야 함)"; free -h | grep -i swap
echo "== firewalld (inactive)"; systemctl is-active firewalld
echo "== SELinux"; getenforce
echo "== 시간 동기화 (Leap status: Normal)"; chronyc tracking | grep -E 'Leap|System time'
echo "== ip_forward (1)"; sysctl -n net.ipv4.ip_forward
echo "== Docker 설치 여부"; systemctl is-active docker 2>/dev/null; docker ps 2>/dev/null | tail -n +2 | wc -l
echo "== nm-cloud-setup (disabled 또는 없음)"; systemctl is-enabled nm-cloud-setup.service nm-cloud-setup.timer 2>/dev/null
echo "== NetworkManager conf"; cat /etc/NetworkManager/conf.d/rke2-canal.conf 2>/dev/null || echo "없음 → 인프라팀 요청"
echo "== 포트 점유 (비어 있어야 함)"; ss -lntup | grep -E ':(80|443|2379|2380|6443|9345|9099|10250)\s'
echo "== 디스크"; df -h / /app
echo "== 아웃바운드"; curl -sI --max-time 5 https://get.rke2.io | head -1 || echo "차단"
echo "== Rancher DNS"; getent hosts rancher.example.internal || echo "미등록"
echo "== 기존 rke2 잔재 (없어야 함)"; ls /etc/rancher /var/lib/rancher 2>/dev/null
```

판단 기준:

- **hostname**이 겹치면 etcd 멤버 이름이 충돌해 조인이 실패합니다.
- **Docker**가 있고 컨테이너가 0개면 끕니다. RKE2는 자체 containerd를 쓰므로 Docker는 쓰이지 않습니다. Docker 데몬은 iptables FORWARD 정책을 DROP으로 바꿔 Pod 통신을 간헐적으로 끊을 수 있습니다. 컨테이너가 돌고 있으면 그 서비스를 옮긴 뒤 끕니다.
- **nm-cloud-setup**이 enabled면 Canal과 충돌합니다. 비활성화 후 재부팅합니다.
- **포트** 80·443은 Traefik hostPort가, 6443·9345는 API와 supervisor가 씁니다. 무엇이든 떠 있으면 기동이 실패합니다.
- **클러스터 내부 대역** 기본값은 Pod 10.42.0.0/16, Service 10.43.0.0/16입니다. 사내에서 이 대역을 쓰는지 인프라팀에 확인합니다. 겹치면 `cluster-cidr`, `service-cidr`를 바꿔야 하고, 설치 후에는 변경할 수 없습니다.

```bash
sudo systemctl disable --now docker docker.socket containerd          # Docker 와 Docker 가 깐 containerd
sudo systemctl disable --now nm-cloud-setup.service nm-cloud-setup.timer
```

## 2. 인프라팀에 요청할 것

내 sudo로 할 수 있어도 OS 설정은 인프라팀 영역이면 요청서로 보냅니다. 그대로 복사해 실행할 수 있는 형태로 주면 처리가 빠릅니다.

### 2-1. NetworkManager가 CNI 인터페이스를 건드리지 않게

RKE2 공식 문서의 RHEL 계열 필수 설정입니다. 없으면 NetworkManager가 Canal이 만든 `flannel*`, `cali*` 인터페이스를 관리 대상으로 잡아 네트워크가 불안정해집니다.

```bash
sudo tee /etc/NetworkManager/conf.d/rke2-canal.conf <<'EOT'
[keyfile]
unmanaged-devices=interface-name:flannel*;interface-name:cali*;interface-name:tunl*;interface-name:vxlan.calico;interface-name:vxlan-v6.calico;interface-name:wireguard.cali;interface-name:wg-v6.cali
EOT
sudo systemctl reload NetworkManager
```

### 2-2. journald 영구 저장 (선택)

기본값은 메모리 저장이라 재부팅하면 `rke2-server` 로그가 사라집니다. 장애 분석을 위해 디스크에 남기고 상한을 둡니다.

```bash
sudo mkdir -p /etc/systemd/journald.conf.d
sudo tee /etc/systemd/journald.conf.d/rke2.conf <<'EOT'
[Journal]
Storage=persistent
SystemMaxUse=2G
EOT
sudo systemctl restart systemd-journald
```

### 2-3. L4, DNS, VIP

| 요청 | 내용 | 확인할 답 |
| --- | --- | --- |
| L4 | VIP:443 → MGMT-1~3:443 | 헬스체크 방식(TCP 443이면 충분), 6443·9345도 넘기는지 |
| DNS | `rancher.example.internal` → L4 VIP | 노드와 Pod 양쪽에서 풀려야 함 |
| dev VIP | DEV 대역에서 미사용 IP 1개 예약 | MetalLB가 ARP로 띄움. DHCP 범위 밖이어야 함 |

### 2-4. 아웃바운드와 사내 방화벽

3장의 표를 요청서 형식에 맞춰 보냅니다.

## 3. 방화벽 허용 리스트

같은 대역 안의 통신(관리 3대끼리, dev 노드끼리)은 제외했습니다. 다른 대역이면 RKE2 노드 간 포트(6443, 9345, 2379-2380, 10250, 8472/udp, 9099)를 추가합니다.

### 인바운드

| Src | Dst | Port | 용도 | 필수 |
| --- | --- | --- | --- | --- |
| 관리자 PC | MGMT-1~3, L4 VIP | TCP/443 | Rancher UI·API | 필수 |
| 관리자 PC | MGMT-1~3 | TCP/22 | 설치·점검 | 설치 시 |
| 관리자 PC | MGMT-1~3 | TCP/6443 | 관리 클러스터 kubectl. PC에서 `helm install rancher`를 치려면 필요 | 필수 |
| 관리자 PC | DEV-1~3 | TCP/22 | 등록 명령 실행 | 설치 시 |
| 관리자 PC | DEV-1~3, dev VIP | TCP/80, 443 | 배포한 서비스 접속 | 테스트 시 |
| 관리자 PC | DEV-1~3 | TCP/6443 | dev kubectl 직접 접속 | 선택. Rancher 경유면 불필요 |
| **DEV-1~3** | **MGMT-1~3, L4 VIP** | **TCP/443** | `rancher-system-agent`, `cattle-cluster-agent`가 Rancher에 접속. WebSocket 상시 연결 | **필수** |
| Jenkins(운영), Jenkins(개발) | MGMT-1~3, L4 VIP | TCP/443 | Rancher 발급 kubeconfig로 배포. 클러스터가 늘어도 변경 없음 | 배포 자동화 시 |
| DEV-1~3 | Nexus | TCP/8081, 5000, 5001 | 사내 앱 이미지·Helm 차트 pull. 8081 UI·Helm, 5000 hosted, 5001 proxy/group | 필수 |

Rancher → dev 노드 방향은 **필요 없습니다.** 에이전트가 먼저 접속하는 구조라 Rancher가 노드로 들어갈 일이 없습니다. Rancher Node Driver로 VM을 직접 만들 때만 22가 필요합니다.

### 아웃바운드 (443)

| Src | Dst | 용도 |
| --- | --- | --- |
| MGMT, DEV | `docker.io`, `registry-1.docker.io`, `auth.docker.io`, `production.cloudflare.docker.com` | 컨테이너 이미지. 설치 후에도 지속 필요 |
| MGMT, DEV | `ghcr.io`, `quay.io` | kube-vip, cert-manager 등 이미지 |
| MGMT | `releases.rancher.com`, `charts.jetstack.io` | Rancher·cert-manager Helm 차트 |
| MGMT | `get.rke2.io`, `github.com`, `objects.githubusercontent.com`, `raw.githubusercontent.com` | RKE2 설치 스크립트·바이너리. 수동 반입하면 제외 가능 |

dev 노드는 Rancher가 RKE2를 설치해 주므로 `get.rke2.io`와 GitHub가 필요 없습니다. 설치 이미지(`rancher/system-agent-installer-rke2`)를 docker.io에서 받으므로 레지스트리 접근은 있어야 합니다.

### 요청서 작성 팁

- **조건부 문구를 빼고 확정형으로** 씁니다. "선택", "VIP 사용 시 포함", "미사용 시 삭제" 같은 표현은 담당자가 되묻게 만듭니다. 쓸 행만 남기고 보냅니다.
- "Rancher ↔ 클러스터 양방향"으로 적지 말고 **노드 → Rancher 443 단방향**으로 방향을 명시합니다.
- Jenkins가 운영·개발 두 대면 설명 칸에 구분을 적어 같은 서버가 두 번 나온 것처럼 보이지 않게 합니다.
- GitLab은 지금 구성에서 필요 없습니다. Jenkins 서버가 소스를 받아 빌드하고 Nexus에 push하며, 클러스터는 Nexus에서 pull만 합니다. Fleet·ArgoCD 같은 GitOps를 쓰거나 Jenkins 빌드 에이전트를 클러스터 Pod로 띄우면 그때 Rancher 또는 dev 노드 → GitLab 행을 추가합니다.
- DNS(53)·NTP(123)는 서버에서 이미 동작을 확인했으면 요청하지 않습니다.
- Nexus를 Docker Hub proxy로 쓰면 외부 docker.io 행을 지울 수 있습니다. 다만 proxy 장애 대비로 남겨도 됩니다.

## 4. RKE2 데이터를 `/app` 아래에 두기: 바인드 마운트

인프라팀이 "RKE2는 `/app` 아래에 설치해 달라"고 요청하는 경우가 있습니다. `data-dir` 옵션으로 경로를 바꾸면 모든 문서의 경로가 달라져 운영이 번거롭습니다. 대신 **정석 경로를 유지하고 디스크만 `/app`을 쓰도록** 바인드 마운트합니다. Rancher가 설치하는 dev 노드도 경로가 같아서 동일하게 적용됩니다.

```bash
# 1) 실제 저장 디렉터리 (/app은 계정 권한이라 sudo 불필요)
mkdir -p /app/rke2/rancher /app/rke2/kubelet /app/rke2/log-pods

# 2) 마운트 지점
sudo mkdir -p /var/lib/rancher /var/lib/kubelet /var/log/pods

# 3) fstab 등록. 칸 사이 공백 수는 자유. EOT는 줄 맨 앞에
sudo tee -a /etc/fstab <<'EOT'
/app/rke2/rancher    /var/lib/rancher   none  bind  0 0
/app/rke2/kubelet    /var/lib/kubelet   none  bind  0 0
/app/rke2/log-pods   /var/log/pods      none  bind  0 0
EOT

# 4) 마운트와 확인
sudo systemctl daemon-reload
sudo mount -a
findmnt | grep rke2
```

| 경로 | 내용 | 용량 |
| --- | --- | --- |
| `/var/lib/rancher` | RKE2 바이너리, 이미지, etcd, 매니페스트 | 가장 큼. 수십 GB |
| `/var/lib/kubelet` | Pod 임시 볼륨, emptyDir | 워크로드에 따라 |
| `/var/log/pods` | 컨테이너 로그 | 로테이션됨 |

주의할 점:

- **RKE2 설치 전에** 끝내야 합니다. 설치 후 마운트하면 기존 내용이 가려져 클러스터가 깨집니다.
- `findmnt`에 인자를 두 개 이상 주면 "장치 + 마운트 지점"으로 해석해 빈 출력이 나옵니다. 하나씩 확인하거나 `findmnt | grep rke2`를 씁니다.
- 바인드 마운트 아래에 만드는 파일(6장의 kubelet 드롭인 등)은 마운트 **뒤에** 만듭니다.

## 5. 관리 서버: sudo가 필요한 작업 순서

| 순서 | 작업 | sudo |
| --- | --- | --- |
| 0 | 4장 바인드 마운트 | O |
| 1 | RKE2 설치 스크립트, `/etc/rancher/rke2/config.yaml` | O |
| 2 | Traefik hostPort 매니페스트 (MGMT-1만) | O |
| 3 | `systemctl enable --now rke2-server` (MGMT-1 → Ready 확인 → 2 → 3) | O |
| 4 | kubeconfig 복사 `sudo cat /etc/rancher/rke2/rke2.yaml > ~/.kube/config` | O (한 번) |
| 5 | PATH 등록, cert-manager·Rancher Helm 설치 | X |

sudo를 요청할 때 "설치 기간 동안 `/etc/rancher`, `/var/lib/rancher`, `systemctl` 범위"로 좁혀 말하면 받기 쉽습니다. 설치가 끝나면 일상 운영은 kubectl·helm이라 내 계정으로 됩니다.

### config.yaml (MGMT-1)

```yaml
token: ${RKE2_TOKEN}            # openssl rand -hex 32 로 생성, 3대 동일
tls-san:
  - 10.0.0.11
  - 10.0.0.12
  - 10.0.0.13
  - 10.0.0.10                   # L4 VIP
  - rancher.example.internal
cni: canal
write-kubeconfig-mode: "0640"
etcd-expose-metrics: true
etcd-snapshot-schedule-cron: "0 */6 * * *"
etcd-snapshot-retention: 20
```

MGMT-2·3은 위 내용에 `server: https://10.0.0.11:9345`를 추가합니다. L4가 9345를 넘겨 주면 VIP 주소를 씁니다. `cluster-cidr`, `service-cidr`는 기본값을 쓰면 적지 않습니다. 1장에서 사내 대역과 겹치는 것이 확인됐을 때만 다른 값으로 넣습니다.

### Traefik을 L4 뒤에 두는 설정 (MGMT-1, 기동 전)

RKE2 Traefik 차트 기본값은 Service `LoadBalancer`인데 온프레미스에는 IP를 줄 컨트롤러가 없어 `<pending>`으로 남습니다. L4가 노드 IP의 80·443으로 직접 보내므로 DaemonSet + hostPort로 받고 Service는 내부용 ClusterIP로 둡니다.

```yaml
# /var/lib/rancher/rke2/server/manifests/rke2-traefik-config.yaml
apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: rke2-traefik
  namespace: kube-system
spec:
  valuesContent: |-
    deployment:
      kind: DaemonSet
    ports:
      web:
        hostPort: 80
      websecure:
        hostPort: 443
    service:
      type: ClusterIP
```

`type: ClusterIP`는 실제 값입니다. 자리표시자가 아닙니다. root 디렉터리라 `sudo vi`로 열어야 하고, 일반 vi에서 저장하면 `E212: Can't open file for writing`이 납니다. 이미 열었다면 `:w !sudo tee %`로 저장합니다.

### 설치

```bash
curl -sfL https://get.rke2.io | INSTALL_RKE2_VERSION=v1.36.4+rke2r1 sudo sh -
sudo systemctl enable --now rke2-server.service
sudo journalctl -u rke2-server -f                     # "rke2 is up and running"
sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml get nodes
```

아웃바운드가 아직 막혀 있으면 PC에서 `rke2.linux-amd64.tar.gz`, `rke2-images.linux-amd64.tar.zst`, `sha256sum-amd64.txt`, 설치 스크립트를 받아 서버로 옮긴 뒤 `INSTALL_RKE2_ARTIFACT_PATH=/app/rke2-artifacts sudo sh install.sh`로 설치합니다. Rancher·cert-manager 이미지는 여전히 레지스트리 아웃바운드가 필요합니다.

## 6. dev 노드 준비: 관리 서버와 무엇이 다른가

dev 노드는 RKE2를 **Rancher가 설치**합니다. OS 준비까지만 하고 RKE2 자체는 손대지 않습니다.

### 같은 것

1장 점검, Docker 중지, nm-cloud-setup, 포트 점검, NetworkManager conf 요청, 4장 바인드 마운트. 모두 동일합니다.

### 하지 않는 것

- 설치 스크립트를 직접 실행하지 않습니다. 미리 깔면 등록이 꼬입니다.
- `/etc/rancher/rke2/config.yaml`을 만들지 않습니다. Rancher가 `config.yaml.d/50-rancher.yaml`을 생성합니다.
- 매니페스트 파일을 서버에 두지 않습니다.

관리 서버에서 파일로 했던 것은 Rancher UI의 클러스터 생성 화면으로 갑니다.

| 관리 서버 (파일) | dev 클러스터 (Rancher UI) |
| --- | --- |
| `token`, `server` | 등록 명령에 포함 |
| `tls-san` | Advanced → Additional Server Args |
| `cni`, `cluster-cidr`, `service-cidr` | Networking |
| etcd 스냅샷 주기 | etcd |
| Traefik hostPort 매니페스트 | Add-On Config → Additional Manifest |
| `registries.yaml` | Registries 탭 |
| kubelet 인자 | Advanced → Additional Kubelet Args |

### 추가로 필요한 것

**DNS.** 에이전트가 `rancher.example.internal`로 접속하므로 dev 노드에서 풀려야 합니다. 등록이 안 되는 원인 1순위입니다.

```bash
getent hosts rancher.example.internal
curl -sk https://rancher.example.internal/ping      # Rancher 설치 후 pong
```

**스토리지 패키지.** RKE2에는 k3s와 달리 local-path 스토리지가 없습니다. PVC를 쓰려면 Longhorn이나 NFS가 필요하고 OS 패키지가 선행됩니다. sudo가 있을 때 미리 깝니다.

```bash
sudo dnf install -y iscsi-initiator-utils nfs-utils
sudo systemctl enable --now iscsid
sudo modprobe iscsi_tcp && echo iscsi_tcp | sudo tee /etc/modules-load.d/iscsi_tcp.conf
```

**MetalLB VIP.** L4가 없으니 dev 서비스 접점은 MetalLB가 ARP로 띄우는 VIP입니다. 2-3장의 예약 요청이 끝나야 합니다.

**등록 명령.** Rancher UI에서 Custom 클러스터를 만들면 나오는 한 줄을 각 노드에서 sudo로 실행합니다. 세 대 모두 `--etcd --controlplane --worker`로 겸임시킵니다.

### swap을 끄지 않고 쓰기 (dev만)

Kubernetes 1.34부터 swap 지원(NodeSwap)이 정식 기능입니다. 관리 서버는 끄고, dev는 켜 둔 채 `LimitedSwap`으로 운영할 수 있습니다. 조건은 cgroup v2(Rocky 9 기본)와 kubelet 설정 두 가지입니다.

`LimitedSwap`은 QoS가 **Burstable**인 Pod만 swap을 쓰게 합니다. Guaranteed, BestEffort, 시스템 Pod(etcd, API 서버 등)는 swap을 쓰지 않습니다. dev 노드가 etcd 노드이기도 해서 Kubernetes 문서는 컨트롤 플레인의 swap을 권하지 않지만, 시스템 Pod가 swap 대상에서 빠지므로 dev에서는 시도할 만합니다.

서버에서 (sudo, 바인드 마운트 뒤):

```bash
echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/90-swap.conf
sudo sysctl --system

sudo mkdir -p /var/lib/rancher/rke2/agent/etc/kubelet.conf.d
sudo tee /var/lib/rancher/rke2/agent/etc/kubelet.conf.d/90-swap.conf <<'EOT'
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
memorySwap:
  swapBehavior: LimitedSwap
EOT
```

Rancher UI에서 Additional Kubelet Args에 `fail-swap-on=false`를 넣습니다. 이게 없으면 swap이 켜진 노드에서 kubelet이 기동을 거부합니다. `swapBehavior`는 kubelet 플래그가 없어 설정 파일 드롭인으로만 넣을 수 있습니다.

클러스터가 뜬 뒤 확인:

```bash
kubectl get --raw "/api/v1/nodes/<노드명>/proxy/configz" | grep -o '"swapBehavior":"[A-Za-z]*"'
sudo ls /var/lib/rancher/rke2/agent/etc/kubelet.conf.d/     # RKE2가 만든 00-*.conf 와 같은 자리인지
```

효과를 보려면 앱 Deployment에 `requests.memory`를 `limits.memory`보다 작게 적어 Burstable로 만들어야 합니다.

## 7. registries.yaml과 Nexus push/pull

빌드·배포 흐름이 "Jenkins가 Nexus에 push → 노드가 Nexus에서 pull"로 바뀌면 **pull 쪽**에 설정이 필요합니다. push는 Jenkins 서버의 Docker 설정이라 클러스터와 무관합니다.

노드의 containerd는 레지스트리 주소를 기본으로 HTTPS로 접근합니다. Nexus가 HTTP면 "server gave HTTP response to HTTPS client"로 pull이 실패합니다. 기존 Docker의 `insecure-registries`에 해당하는 설정이 `registries.yaml`입니다.

```yaml
# Nexus 가 HTTP 인 경우
mirrors:
  "nexus.example.internal:5000":
    endpoint:
      - "http://nexus.example.internal:5000"
  docker.io:                                   # Docker Hub proxy 로 돌릴 때만
    endpoint:
      - "http://nexus.example.internal:5001"
configs:
  "nexus.example.internal:5000":
    auth:
      username: ${NEXUS_USER}
      password: ${NEXUS_PASSWORD}
```

| 블록 | 역할 |
| --- | --- |
| `mirrors` | 어느 주소로 받을지. HTTP 지정, docker.io를 Nexus proxy로 우회 |
| `configs` | 인증과 TLS. 익명 pull 허용 저장소면 생략 |

docker.io mirror는 endpoint가 실패하면 원래 docker.io로 자동 재시도합니다. Nexus가 죽어도 아웃바운드만 열려 있으면 공개 이미지는 받아 옵니다.

- **관리 서버**: 사내 앱이 올라가지 않으므로 생략해도 됩니다.
- **dev 클러스터**: 필요합니다. 서버에 파일을 만들지 않고 Rancher UI의 Registries 탭에 입력하면 모든 노드에 내려갑니다.
- 네임스페이스마다 `imagePullSecret`을 두는 방법도 있지만 HTTP 문제는 해결하지 못하므로 어차피 `registries.yaml`이 필요합니다.

## 8. 체크리스트

관리 서버 (MGMT-1~3):

- [ ] 1장 점검 결과가 모두 기대값인가
- [ ] NetworkManager conf, journald, L4, DNS, 아웃바운드, sudo를 인프라팀에 요청했고 답을 받았는가
- [ ] 10.42/10.43 대역이 사내와 겹치지 않는가
- [ ] 바인드 마운트 3개가 `findmnt | grep rke2`에 보이는가
- [ ] config.yaml 3개에 같은 token, MGMT-2·3에 `server:`가 있는가
- [ ] MGMT-1에 Traefik HelmChartConfig가 있고 네 최상위 키가 같은 열에서 시작하는가

dev 노드 (DEV-1~3):

- [ ] 1장 점검, Docker 중지, 바인드 마운트
- [ ] `rancher.example.internal`이 노드에서 풀리는가
- [ ] 스토리지 패키지, swap 드롭인, swappiness
- [ ] MetalLB VIP를 예약받았는가
- [ ] 방화벽: DEV → Rancher 443, DEV → Nexus, DEV → 레지스트리 아웃바운드
- [ ] RKE2 설치 스크립트와 config.yaml을 **만들지 않았는가**

## 관련 문서

- 권한이 있는 서버에서의 전체 설치 절차: [15 RKE2 3대 클러스터 구축](15-rke2-three-node-build.md)
- Rancher 설계와 에이전트 동작, dev/stage/prod 배치: [16 Rancher로 여러 클러스터 중앙 관리](16-rancher-multi-cluster-management.md)
- RKE2 설정 키 의미: [14 RKE2](14-rke2-distribution.md)
- 같은 구조를 로컬에서 재현: [Lima + Rancher + RKE2 실습 매뉴얼](../commands/lima-rancher-rke2-lab.md)
- Ingress와 L4의 역할: [09 외부 트래픽 진입점](09-external-traffic-entrypoint.md)

## 참고한 공식 문서

- RKE2 Documentation — Requirements(Known Issues: NetworkManager, nm-cloud-setup, firewalld), Air-Gap Install(`INSTALL_RKE2_ARTIFACT_PATH`), Private Registry Configuration, Networking(Traefik HelmChartConfig) (2026-10-12 확인, v1.36 기준)
- Rancher Manager Documentation — Installation Requirements(Rocky 9.8, RKE2 1.34~1.36), Port Requirements, Custom Clusters (Rancher v2.15 기준)
- Kubernetes Documentation — Swap memory management(NodeSwap, LimitedSwap), Kubelet Configuration drop-in directory
- Red Hat — journald.conf `Storage`, `SystemMaxUse`

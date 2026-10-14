# Lima + Rancher + RKE2 실습 매뉴얼

macOS(Apple Silicon)에서 Lima VM 두 대로 **관리 클러스터(RKE2 + Rancher)와 dev 클러스터(RKE2, Custom 등록)**를 구성하는 절차입니다. 온프레미스 구축 순서와 1:1로 대응되도록 짰습니다. 설계 배경은 [16 Rancher로 여러 클러스터 중앙 관리](../concepts/16-rancher-multi-cluster-management.md)에 있습니다.

k3d가 아니라 VM을 쓰는 이유는 RKE2가 systemd가 있는 Linux 호스트를 전제로 하기 때문입니다. k3d 클러스터는 Rancher에 Import만 가능하고, 등록 명령으로 RKE2를 설치하는 Custom 흐름을 볼 수 없습니다. k3d 기반 실습은 [k3d 실습 매뉴얼](k3d-manual.md)을 봅니다.

## 구성

```text
Mac
 ├─ Safari ──────────────────────────────────┐
 ├─ :8443 (Lima 포트포워드, static) ─────────┼─▶ mgmt VM :443 (Traefik → Rancher)
 │                                           │
 ├─ mgmt VM  192.168.64.2  (vzNAT)           │
 │    RKE2 server + Rancher + cert-manager   │
 │                                           │
 └─ dev1 VM  192.168.64.3  (vzNAT) ──────────┘
      rancher-system-agent → RKE2 (Rancher가 설치)
      dev1 → Mac(192.168.64.1):8443 → mgmt   ← VM 간 직접 통신이 막혀 Mac을 경유

Rancher hostname: rancher.192-168-64-1.sslip.io:8443
```

| 항목 | 값 | 비고 |
| --- | --- | --- |
| Lima | 2.2.0 | `brew install lima`. sudo 불필요 |
| VM 이미지 | Ubuntu 24.04 arm64 | |
| mgmt VM | 3 vCPU, 6GB, 30GB | 4GB로는 며칠 뒤 API 서버가 느려져 Rancher와 webhook이 재시작을 반복했다 |
| dev1 VM | 2 vCPU, 5GB, 30GB | RKE2 + Calico + Rancher 에이전트만으로 2.4GB를 쓴다. 3GB면 실습 Pod를 올릴 여유가 없다 |
| RKE2 | v1.36.4+rke2r1 | Ingress는 Traefik (v1.36 기본) |
| Rancher | v2.15.2, `replicas=1` | 실제 구축은 3노드에 `replicas=3` |
| hostname | sslip.io | IP를 이름에 넣으면 공개 DNS가 그 IP로 풀어주는 서비스. 사내 DNS 대용 |

Docker Desktop은 종료합니다. VM과 메모리를 겹쳐 쓰기 때문입니다. 2026-10-02 기준으로 검증했습니다.

## 1. mgmt VM 생성

```bash
mkdir -p ~/lima-rancher && cd ~/lima-rancher
```

```yaml
# ~/lima-rancher/mgmt.yaml
vmType: vz
cpus: 3
memory: "6GiB"
disk: "30GiB"
images:
  - location: https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-arm64.img
    arch: aarch64
networks:
  - vzNAT: true
portForwards:
  - guestPort: 443
    hostIP: "0.0.0.0"
    hostPort: 8443
    static: true
```

`vzNAT`는 VM에 Mac에서 직접 닿는 IP(`192.168.64.x`)를 줍니다. `portForwards`의 `static: true`는 뒤에서 설명하는 VM 간 통신 차단을 우회하는 데 필요합니다. 처음부터 넣어두면 재시작을 한 번 줄입니다.

```bash
limactl start --name mgmt mgmt.yaml --tty=false
limactl shell mgmt -- ip -4 addr show lima0 | grep inet      # 192.168.64.2
```

### 외부 통신 경로 수정

사내 보안 도구(VPN, 네트워크 필터)가 있는 Mac에서는 vzNAT 경로의 외부 통신이 막힐 수 있습니다. Lima VM에는 인터페이스가 두 개 있고, user-mode 경로(`eth0`)는 Mac의 일반 프로세스처럼 나가므로 대부분 뚫려 있습니다.

```bash
limactl shell mgmt
curl -sS -m 10 -o /dev/null -w '%{http_code}\n' https://get.rke2.io                     # timeout 이면 막힌 것
curl -sS -m 10 --interface eth0 -o /dev/null -w '%{http_code}\n' https://get.rke2.io    # 200 이면 eth0 는 됨
```

`eth0`로만 되면 기본 경로를 `eth0`로 바꿉니다. `lima0`는 Mac↔VM 통신용으로 남습니다.

```bash
cat <<EOF | sudo tee /etc/netplan/99-lima0-metric.yaml
network:
  version: 2
  ethernets:
    lima0:
      dhcp4: true
      dhcp4-overrides:
        route-metric: 300
EOF
sudo chmod 600 /etc/netplan/99-lima0-metric.yaml
sudo netplan apply
ip route | grep default        # eth0 가 200, lima0 가 300
```

## 2. mgmt VM에 RKE2 직접 설치

Rancher가 아직 없으니 이 VM만 수동 설치입니다. hostname은 **Mac의 vzNAT 게이트웨이 주소(192.168.64.1)** 기준으로 정합니다. dev VM이 Mac을 경유해 들어오기 때문입니다.

```bash
export RANCHER_HOST=rancher.192-168-64-1.sslip.io
echo "export RANCHER_HOST=${RANCHER_HOST}" >> ~/.bashrc

sudo mkdir -p /etc/rancher/rke2
cat <<EOF | sudo tee /etc/rancher/rke2/config.yaml
tls-san:
  - ${RANCHER_HOST}
  - 192.168.64.2
write-kubeconfig-mode: "0644"
EOF

curl -fL -o rke2-install.sh https://get.rke2.io
sudo sh rke2-install.sh
sudo systemctl enable --now rke2-server
```

`write-kubeconfig-mode`가 없으면 RKE2가 재시작할 때마다 kubeconfig 권한을 600으로 되돌려 일반 사용자의 kubectl이 막힙니다.

2~3분 뒤 확인합니다.

```bash
echo 'export KUBECONFIG=/etc/rancher/rke2/rke2.yaml' >> ~/.bashrc
echo 'export PATH=$PATH:/var/lib/rancher/rke2/bin' >> ~/.bashrc
source ~/.bashrc

kubectl get nodes                                 # Ready, control-plane,etcd
kubectl -n kube-system get pods | grep traefik    # rke2-traefik-xxxxx Running
kubectl get ingressclass                          # traefik (default)
```

`limactl shell mgmt -- kubectl ...` 형태는 `~/.bashrc`를 읽지 않아 `kubectl: command not found`가 납니다. 셸에 들어가서 실행하거나 전체 경로를 씁니다.

## 3. Rancher 설치

```bash
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

helm repo add jetstack https://charts.jetstack.io
helm repo add rancher-stable https://releases.rancher.com/server-charts/stable
helm repo update

helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true --wait

helm install rancher rancher-stable/rancher \
  --namespace cattle-system --create-namespace \
  --set hostname=${RANCHER_HOST} \
  --set replicas=1 \
  --set bootstrapPassword=admin1234

kubectl -n cattle-system rollout status deploy/rancher --timeout=10m
kubectl -n cattle-system get ingress          # HOSTS 에 rancher.192-168-64-1.sslip.io
```

`--wait`와 `rollout status`는 이미지를 받는 동안 출력이 없습니다. 멈춘 것이 아니므로 `Ctrl+C`를 누르지 않습니다. 네임스페이스 이름을 잘못 쳐서 취소했다면 CRD에 잘못된 소유 정보가 남아 재설치가 거부됩니다. 아래로 정리합니다.

```bash
helm uninstall cert-manager -n <잘못된 네임스페이스>
kubectl get crd -o name | grep cert-manager.io | xargs kubectl delete
kubectl delete namespace <잘못된 네임스페이스>
```

`replicas`나 `bootstrapPassword`를 빼고 설치했다면 `helm upgrade --reuse-values --set ...`로 덮어씁니다. 단, `bootstrapPassword`는 Rancher가 처음 뜰 때 한 번만 적용되므로 이미 떴다면 기본값 `admin`으로 로그인합니다.

## 4. Mac에서 접속

```bash
# Mac
lsof -nP -iTCP:8443 -sTCP:LISTEN                                  # limactl 이 8443 을 열고 있어야 함
curl -sk -m 5 https://rancher.192-168-64-1.sslip.io:8443/ping     # pong
```

Safari에서 `https://rancher.192-168-64-1.sslip.io:8443`을 엽니다.

1. 인증서 경고 → 세부사항 보기 → 이 웹 사이트 방문
2. `admin1234`(또는 `admin`)로 로그인 → 새 admin 비밀번호 설정
3. Server URL을 `https://rancher.192-168-64-1.sslip.io:8443`으로 확인하고 Continue. **`:8443`이 빠지면 등록 명령이 443으로 나가 dev VM에서 연결이 안 됩니다.** 나중에 바꾸려면 ☰ → Global Settings → `server-url`

Chrome에서 `ERR_ADDRESS_UNREACHABLE`이 나고 Safari는 되는 경우가 있습니다. Chrome의 보안 DNS(DoH)나 회사 정책이 사설 IP로 풀리는 이름을 막는 것입니다. `chrome://settings/security`에서 보안 DNS를 끄거나 Safari로 진행합니다.

## 5. dev1 VM 생성

netplan 수정을 `provision`에 넣어 VM이 뜰 때 자동 적용되게 합니다.

```yaml
# ~/lima-rancher/dev1.yaml
vmType: vz
cpus: 2
memory: "5GiB"
disk: "30GiB"
images:
  - location: https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-arm64.img
    arch: aarch64
networks:
  - vzNAT: true
provision:
  - mode: system
    script: |
      #!/bin/bash
      cat > /etc/netplan/99-lima0-metric.yaml <<EOF
      network:
        version: 2
        ethernets:
          lima0:
            dhcp4: true
            dhcp4-overrides:
              route-metric: 300
      EOF
      chmod 600 /etc/netplan/99-lima0-metric.yaml
      netplan apply
```

```bash
limactl start --name dev1 dev1.yaml --tty=false

limactl shell dev1 -- ip route | grep default                                             # eth0 200, lima0 300
limactl shell dev1 -- curl -m 10 -o /dev/null -w '%{http_code}\n' https://get.rke2.io     # 200
limactl shell dev1 -- curl -sk -m 5 https://rancher.192-168-64-1.sslip.io:8443/ping       # pong
```

세 번째 줄이 `pong`이면 dev1 → Mac → mgmt 경로가 열린 것입니다.

## 6. Rancher에서 Custom 클러스터 생성

Safari에서:

1. ☰ → Cluster Management → Create → 하단 **Custom**
2. Cluster Name `dev`, Kubernetes Version은 `+rke2r1`로 끝나는 기본 선택값, 나머지 기본값 → Create
3. Registration 탭 → Node Role에서 **etcd, Control Plane, Worker 모두 체크**
4. **Insecure** 체크. 자체 서명 인증서이므로 `--insecure`와 `--ca-checksum`이 명령에 붙는다
5. 명령 복사. `--server https://rancher.192-168-64-1.sslip.io:8443`인지 확인

## 7. dev1에서 등록 명령 실행

```bash
limactl shell dev1
# 복사한 명령 붙여넣기. 형태:
curl --insecure -fL https://rancher.192-168-64-1.sslip.io:8443/system-agent-install.sh | sudo sh -s - \
  --server https://rancher.192-168-64-1.sslip.io:8443 --label 'cattle.io/os=linux' \
  --token xxxx --ca-checksum xxxx \
  --etcd --controlplane --worker
```

명령은 수십 초 안에 끝납니다. 이 시점에 설치된 것은 에이전트뿐이고 RKE2는 아직 없습니다. 에이전트가 Rancher에서 plan을 받아 RKE2를 설치하는 과정은 로그로 봅니다.

```bash
sudo systemctl status rancher-system-agent --no-pager | head -3    # active (running)
sudo journalctl -u rancher-system-agent -f                         # RKE2 다운로드·설치 로그
sudo systemctl status rke2-server --no-pager | head -3             # 1~3분 뒤 active (running)
```

Rancher UI → Cluster Management → dev → Machines에서 dev1이 `Waiting → Provisioning → Running`으로 바뀌고 클러스터가 `Active`가 되면 완료입니다. 5분 안팎 걸립니다.

에이전트가 만든 RKE2 설정은 `config.yaml`이 아니라 디렉터리 안에 있습니다.

```bash
ls /etc/rancher/rke2/                            # config.yaml.d  registries.yaml  rke2-pss.yaml  rke2.yaml
sudo cat /etc/rancher/rke2/config.yaml.d/50-rancher.yaml
```

## 8. Mac에서 dev 클러스터 사용

Safari → Cluster Management → `dev` ⋮ → Download KubeConfig. 이 kubeconfig는 Rancher를 경유하므로 dev1에 직접 접근하지 않아도 됩니다.

```bash
# 기존 설정에 합쳐서 context 로 전환
cp ~/.kube/config ~/.kube/config.bak
KUBECONFIG=~/.kube/config:~/Downloads/dev.yaml kubectl config view --flatten > ~/.kube/config.new
mv ~/.kube/config.new ~/.kube/config

kubectl config use-context dev
kubectl get nodes -o wide        # lima-dev1  control-plane,etcd,master,worker
```

kubectl은 원격 조종기이므로 Mac, Rancher UI의 브라우저 터미널(클러스터 화면 오른쪽 상단 `>_`), dev1 VM 안 어디서 실행해도 결과가 같습니다. 실무에서는 운영자 PC에서 실행하고, 노드에 들어가는 일은 거의 없습니다.

## 9. 실습 과제

### 워크로드 올리고 UI에서 내리고 올리기

```bash
kubectl create ns myapp
kubectl -n myapp create deploy web-portal --image=nginx:1.27 --replicas=2
kubectl -n myapp create deploy was-order --image=tomcat:10.1 --replicas=2
kubectl -n myapp create deploy was-member --image=tomcat:10.1 --replicas=1
kubectl -n myapp get pods -w
```

Safari → dev → Workloads → Deployments에서 `was-order`만 Scale 0으로 내렸다가 다시 올립니다. 다른 Deployment는 영향이 없습니다. Redeploy, Rollback, View Logs, Execute Shell, Edit YAML도 눌러봅니다.

### 도메인별 Ingress 분기

```bash
kubectl -n myapp expose deploy web-portal --port=80
kubectl -n myapp expose deploy was-order --port=8080
kubectl -n myapp create ingress portal --class=traefik \
  --rule='portal.dev.192-168-64-3.sslip.io/*=web-portal:80'
kubectl -n myapp create ingress order --class=traefik \
  --rule='order.dev.192-168-64-3.sslip.io/*=was-order:8080'

curl -s http://portal.dev.192-168-64-3.sslip.io/ | grep title                      # nginx
curl -s -o /dev/null -w '%{http_code}\n' http://order.dev.192-168-64-3.sslip.io/   # tomcat 404
```

같은 IP(dev1)로 들어왔는데 호스트명으로 다른 백엔드에 닿습니다. Mac → dev1 직접 경로는 되므로 포워드가 필요 없습니다.

### Project와 권한

1. dev → Projects/Namespaces → Create Project `order-team` → `myapp`을 이 Project로 이동
2. Users & Authentication → Users → `dev-user` 생성 (Standard User)
3. `order-team` → Members → `dev-user`를 Project Member로 추가
4. Safari 시크릿 창에서 `dev-user`로 로그인하면 `myapp`만 보이고 `kube-system`은 보이지 않음

### 노드 제어

dev → Nodes → dev1 ⋮ → Cordon → `kubectl get nodes`에 `SchedulingDisabled`. Scale을 올려도 새 Pod가 `Pending`에 머무는 것을 확인한 뒤 Uncordon. Drain은 단일 노드라 Pod가 갈 곳이 없어 실패합니다. 그 메시지 자체가 "노드 1대로는 무중단 유지보수가 안 된다"는 교훈입니다.

### 노드 추가 (메모리 여유가 있으면)

`dev1.yaml`을 `dev2.yaml`로 복사해 띄우고, Registration 탭에서 **Worker만 체크**한 명령을 dev2에서 실행합니다. 등록 뒤 Drain을 다시 하면 Pod가 dev2로 옮겨갑니다. 끝나면 `limactl stop dev2`로 메모리를 돌려줍니다.

### Rancher가 해주는 운영 작업

- Cluster Management → dev ⋮ → Edit Config → Kubernetes Version 변경 → dev1에서 `journalctl -u rancher-system-agent -f`로 RKE2 교체 과정 관찰
- Cluster Management → dev → Snapshots → Take Snapshot → dev1의 `/var/lib/rancher/rke2/server/db/snapshots/`에 파일 생성 확인

### Rancher 장애 시 동작

```bash
limactl stop mgmt
curl -s http://portal.dev.192-168-64-3.sslip.io/ | grep title    # 여전히 응답
kubectl get nodes                                                   # 실패 (Rancher 경유 kubeconfig)
limactl start mgmt
```

Rancher가 없어도 서비스는 되고, Rancher 경유 kubectl만 안 됩니다.

## 10. 막힌 지점과 원인

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| VM에서 `curl https://get.rke2.io` timeout, DNS는 됨 | 사내 보안 도구가 vzNAT 외부 통신 차단 | `--interface eth0`로 되는지 확인 후 netplan으로 `lima0` metric을 300으로 |
| dev1에서 mgmt(192.168.64.2) `Destination Host Unreachable`, 게이트웨이(.1)는 됨 | 같은 도구가 VM 간 브리지 트래픽 차단 | Mac을 경유. mgmt에 `portForwards` 8443→443, hostname을 `192-168-64-1` 기준으로 |
| Mac에서 8443 LISTEN 없음. 로그에 `Found non-static port forward` | Traefik은 443을 소켓이 아닌 iptables로 받아 Lima가 리스닝을 감지하지 못함 | `portForwards`에 `static: true` |
| `~/.lima/mgmt/lima.yaml`에 `unknown field "hostIp"` 경고 | 키 대소문자와 들여쓰기 오류 | `hostIP`, `- guestPort` 아래 4칸 들여쓰기 |
| `kubectl -n kube-system get pods \| grep ingress` 결과 없음 | RKE2 v1.36은 ingress-nginx가 아니라 Traefik | `grep traefik`, IngressClass `traefik` |
| `rancher-webhook` CrashLoopBackOff, 로그에 `Failed to renew lease ... context deadline exceeded` | VM 자원 부족으로 API 서버 응답 지연. webhook은 리더 자격을 잃으면 종료 | mgmt VM을 3 vCPU, 6GB로 증설 |
| 재시작 후 `rke2.yaml: permission denied` | RKE2가 kubeconfig 권한을 600으로 재설정 | `config.yaml`에 `write-kubeconfig-mode: "0644"` |
| `limactl shell mgmt -- kubectl` → `command not found` | 비로그인 셸은 `~/.bashrc`를 읽지 않음 | 셸에 들어가서 실행, 또는 `/var/lib/rancher/rke2/bin/kubectl` 전체 경로 |
| `helm install` 재시도 시 `invalid ownership metadata` | 오타 난 네임스페이스로 설치가 시작돼 CRD에 소유 정보가 남음 | 잘못된 release와 CRD, 네임스페이스 삭제 후 재설치 |
| `curl -sfL https://getrke2.io` 반응 없음 | URL 오타. `-sf`가 오류를 숨김 | `get.rke2.io`. 진단할 때는 `-s` 제거 |
| Chrome `ERR_ADDRESS_UNREACHABLE`, Safari는 됨 | Chrome 보안 DNS나 회사 정책이 사설 IP 응답 차단 | 보안 DNS 끄기 또는 Safari |
| Pod `CrashLoopBackOff`, 로그 `exec format error` | x86 Jenkins가 만든 amd64 이미지를 arm64 VM이 실행 | dev1에 Rosetta(11장) 또는 멀티아키 빌드 |
| nginx Pod `CrashLoopBackOff`, 로그 `host not found in upstream` | 정적 `proxy_pass`가 기동 시 DNS를 요구하는데 대상 Service가 없음 | 변수 `proxy_pass` + `resolver`로 요청 시점 해석 (예제 `web/default.conf.template`) |
| dev1에서 사내 Nexus 5000 timeout, 8081은 401 | PC 대역에서 Docker 포트만 방화벽 차단 | Nexus에 닿는 서버를 경유하는 SSH 터널 + Registries mirror(11장) |
| `crictl` → `connect: no such file or directory` | 기본 소켓 경로가 RKE2와 다름 | `--runtime-endpoint unix:///run/k3s/containerd/containerd.sock` |

이 중 처음 세 줄은 로컬 보안 도구 때문에 생긴 실습 한정 우회입니다. 실제 온프레미스에서는 노드 간 통신이 당연히 되므로 LB 주소 하나로 끝나고 포워딩이 필요 없습니다.

## 11. 사내 Jenkins·Nexus 이미지로 배포하기

9장까지는 공개 이미지였습니다. 실제 흐름(Jenkins가 빌드·push, Rancher에서 릴리즈 선택)을 로컬에서 끝까지 보려면 dev1이 사내 Nexus의 이미지를, Rancher가 사내 Nexus의 차트를 읽어야 합니다. 샘플은 [web/WAS 릴리즈 예제](../examples/web-was-release/README.md)를 씁니다. 아래 주소는 예시입니다.

### 통신 확인

```bash
limactl shell mgmt -- curl -s -m 5 -o /dev/null -w '%{http_code}\n' http://nexus.example.internal:8081/repository/helm-hosted/index.yaml
limactl shell dev1 -- curl -s -m 5 -o /dev/null -w '%{http_code}\n' http://nexus.example.internal:5000/v2/
```

`401`은 통신이 되고 인증만 필요하다는 뜻입니다. `000`(timeout)은 막힌 것입니다. 사내 방화벽은 보통 PC 대역에 Nexus UI 포트(8081)만 열고 Docker 포트(5000)는 서버 대역에만 열어 둡니다. VM은 Mac을 거쳐 나가므로 PC와 같은 결과를 받습니다.

### 이미지: SSH 터널로 우회하고 mirror로 연결

Nexus 5000이 통하는 서버(예: Jenkins 서버)를 경유합니다. Mac 5000은 AirPlay가 점유하므로 15000을 씁니다. `0.0.0.0`으로 열어야 dev1이 Mac(192.168.64.1)으로 들어올 수 있습니다.

```bash
ssh -N -L 0.0.0.0:15000:nexus.example.internal:5000 <user>@<jenkins-server>     # 별도 탭에서 켜 둠
limactl shell dev1 -- curl -s -o /dev/null -w '%{http_code}\n' http://192.168.64.1:15000/v2/   # 401
```

Rancher → Cluster Management → `dev` ⋮ → Edit Config → **Registries**:

| 항목 | 값 |
| --- | --- |
| Mirrors → Registry Hostname | `nexus.example.internal:5000` |
| Mirrors → Mirror Endpoints | `http://192.168.64.1:15000` |
| Registry Authentication → Registry Hostname | `192.168.64.1:15000` |
| Registry Authentication → Authentication | *Create an HTTP Basic Auth Secret* → Nexus 계정 |

Save 후 Rancher가 노드의 `/etc/rancher/rke2/registries.yaml`을 갱신하고 RKE2를 재시작합니다. 이 mirror 덕에 차트와 매니페스트의 이미지 이름은 `nexus.example.internal:5000/...` 그대로 두고 노드만 터널로 우회합니다. 방화벽이 열리면 Endpoint를 실제 주소로 바꾸고 터널을 끄면 끝입니다.

```bash
# 노드에서 직접 pull 테스트. RKE2 containerd 소켓을 지정해야 crictl 이 동작한다
limactl shell dev1 -- sudo /var/lib/rancher/rke2/bin/crictl \
  --runtime-endpoint unix:///run/k3s/containerd/containerd.sock pull nexus.example.internal:5000/web-portal:1.0.9
```

### 차트: Apps 저장소 등록

`dev` → Apps → Repositories → Add Repository: Target **Helm Repository**, Index URL `http://nexus.example.internal:8081/repository/helm-hosted/`, Authentication *Create a HTTP Basic Auth Secret*. Active가 되면 Charts에 Jenkins가 올린 차트가 보입니다. 새 버전을 올린 뒤에는 ⋮ → Refresh.

### amd64 이미지를 arm64 VM에서: Rosetta

x86 Jenkins가 만든 이미지는 amd64입니다. dev1(arm64)에서 그대로 실행하면 `exec format error`로 Pod가 CrashLoopBackOff에 빠집니다. Jenkins 서버 커널이 오래되면 QEMU 멀티아키 빌드도 등록이 안 됩니다(`binfmt_misc/register: invalid argument`). 로컬에서는 Lima의 Rosetta를 켜는 것이 간단합니다.

```yaml
# ~/.lima/dev1/lima.yaml 에 추가
rosetta:
  enabled: true
  binfmt: true
```

```bash
limactl stop dev1 && limactl start dev1
limactl shell dev1 -- cat /proc/sys/fs/binfmt_misc/rosetta | head -2     # enabled
```

재시작 직후 1~2분은 `rancher-webhook`이 아직 없어 네임스페이스 생성이 거부됩니다. 기존 네임스페이스는 바로 쓸 수 있습니다. 실서버 dev 노드는 x86이라 이 우회가 필요 없습니다.

### 설치와 확인

Apps → Charts → `web-portal` → Install → Namespace `webwas`, values의 `image.registry`를 Nexus 주소로 → Install. `helm install --wait`가 Pod Ready까지 기다리므로 로그가 멈춘 듯 보여도 1분 안에 `SUCCESS`가 나와야 합니다. 10분을 넘기면 Pod가 안 뜨는 것이니 Workloads에서 Pod 상태와 로그를 봅니다.

```bash
curl -s http://app.dev.192-168-64-3.sslip.io/ | grep -o 'web-portal <small>v[^<]*'     # web-portal v1.0.9
```

실패한 Helm 릴리즈 위에 Upgrade는 거부될 수 있습니다. Installed Apps에서 Delete하고 다시 Install하는 것이 깔끔합니다.

## 12. 정리

```bash
limactl stop dev1 mgmt            # 멈추기만. 다음에 start 로 이어서
limactl delete --force dev1 mgmt  # 완전 삭제
```

VM을 삭제하고 다시 만들면 vzNAT IP가 바뀔 수 있습니다. 그때는 hostname과 `tls-san`을 새 IP 기준으로 다시 잡습니다.

## 관련 문서

- 설계 결정 배경: [16 Rancher로 여러 클러스터 중앙 관리](../concepts/16-rancher-multi-cluster-management.md)
- RKE2 부품과 설정 키: [14 RKE2](../concepts/14-rke2-distribution.md)
- 관리 클러스터를 실제 3대로 올리는 절차: [15 RKE2 3대 클러스터 구축](../concepts/15-rke2-three-node-build.md)
- k3d 기반 실습과 Import 방식 Rancher 설치: [k3d 실습 매뉴얼](k3d-manual.md)

## 참고한 공식 문서

- Lima Documentation — Network (vzNAT, user-mode), Port Forwarding (`static`), Provision scripts (2026-10-02 확인, v2.2.0)
- Rancher Manager Documentation — Install on a Kubernetes Cluster, Launching Kubernetes on Existing Custom Nodes, Server URL setting
- RKE2 Documentation — Quick Start, Configuration File, Networking (Traefik)

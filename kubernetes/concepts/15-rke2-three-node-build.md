# RKE2 3대 클러스터 구축 절차

서버 3대로 RKE2 클러스터를 처음부터 끝까지 올리는 절차입니다. [07 실서버 클러스터 토폴로지](07-production-cluster-topology.md)의 "소규모 절충" 구성, 즉 **3대 모두 server(컨트롤 플레인 + 워커 겸임), 내장 etcd, API 고정 접점 하나**를 만듭니다. RKE2 자체의 부품과 설정 의미는 [14 RKE2](14-rke2-distribution.md)에 있으므로 여기서는 반복하지 않고 순서와 명령에 집중합니다.

## 0. 구성과 가정

```text
            k8s-api.example.internal = 10.0.0.10 (kube-vip VIP) ─ 6443 / 9345
                     │
     ┌───────────────┼───────────────┐
  server-1        server-2        server-3
  10.0.0.11       10.0.0.12       10.0.0.13
  etcd            etcd            etcd
  control plane   control plane   control plane
  워크로드        워크로드        워크로드
  Traefik         Traefik         Traefik      ← 진입 VIP 10.0.0.20 (MetalLB)
```

| 항목 | 값 | 이유 |
| --- | --- | --- |
| 역할 | 3대 모두 server, taint 없음 | 3대에서 HA(1대 장애 허용)가 되는 유일한 구성 |
| API 고정 접점 | kube-vip VIP `10.0.0.10` | 사내 L4 LB가 있으면 그것으로 대체. kubeconfig·조인 주소가 개별 서버를 가리키면 안 됨 |
| CNI | Canal (기본) | 생성 후 변경 불가이므로 처음에 확정 |
| Ingress | Traefik (v1.36 기본) | |
| LoadBalancer | MetalLB L2, 풀 `10.0.0.20-29` | RKE2에는 내장 LB가 없음 |
| 스토리지 | Longhorn, 복제 3 | 노드 장애 시 볼륨이 다른 노드에서 살아남 |
| etcd 백업 | 6시간마다 S3(MinIO)로 | 노드 로컬 스냅샷은 노드와 함께 사라짐 |
| CIS 프로파일 | **처음엔 끔**, 파일럿 후 활성화 | `restricted` PSA가 준비 안 된 워크로드를 전부 거부 |

사양은 대당 8 vCPU, 16GB 이상, OS·etcd용 SSD와 Longhorn용 SSD를 따로 두는 것을 권장합니다. 근거는 [14](14-rke2-distribution.md) 4장과 [07](07-production-cluster-topology.md)의 PoC 사양입니다.

## 1. 3대 공통 준비

```bash
# 호스트명은 클러스터 안에서 유일해야 한다
sudo hostnamectl set-hostname server-1          # 2, 3도 각각

# 시간 동기화 (etcd·인증서 검증에 필요)
timedatectl status | grep "synchronized: yes"

# Longhorn 사전 패키지
sudo dnf install -y iscsi-initiator-utils nfs-utils cryptsetup device-mapper   # RHEL 계열
# sudo apt install -y open-iscsi nfs-common cryptsetup dmsetup                  # Ubuntu
sudo systemctl enable --now iscsid
sudo modprobe iscsi_tcp && echo iscsi_tcp | sudo tee /etc/modules-load.d/iscsi_tcp.conf

# Longhorn 데이터 디스크 (XFS 또는 ext4, 별도 SSD)
sudo mkfs.xfs /dev/sdb && sudo mkdir -p /data/longhorn
echo '/dev/sdb /data/longhorn xfs defaults,noatime 0 0' | sudo tee -a /etc/fstab && sudo mount -a
```

방화벽은 RKE2 문서가 firewalld 비활성화를 권장합니다. 켜 둬야 한다면 최소 아래를 엽니다.

```bash
sudo firewall-cmd --permanent --add-port={6443,9345,2379,2380,10250}/tcp   # 클러스터 내부
sudo firewall-cmd --permanent --add-port=8472/udp                          # Canal VXLAN
sudo firewall-cmd --permanent --add-port={80,443}/tcp                      # Ingress 진입
sudo firewall-cmd --permanent --add-port=3260/tcp                          # Longhorn iSCSI (노드 간)
sudo firewall-cmd --permanent --add-port=7946/{tcp,udp}                    # MetalLB speaker (노드 간)
sudo firewall-cmd --reload
```

토큰은 미리 정해 3대에 같은 값을 씁니다. 복구 시에도 필요하므로 비밀 저장소에 보관합니다.

```bash
openssl rand -hex 32     # → RKE2_TOKEN
```

## 2. server-1: kube-vip를 먼저 두고 기동

RKE2는 `/var/lib/rancher/rke2/server/manifests/` 아래 YAML을 기동 시 자동 배포합니다. kube-vip를 여기 넣어 두면 첫 기동과 함께 VIP가 올라와, 이후 노드가 VIP로 조인할 수 있습니다.

```bash
sudo mkdir -p /etc/rancher/rke2 /var/lib/rancher/rke2/server/manifests
curl -sL https://kube-vip.io/manifests/rbac.yaml \
  | sudo tee /var/lib/rancher/rke2/server/manifests/kube-vip-rbac.yaml >/dev/null
```

DaemonSet 매니페스트는 kube-vip 컨테이너의 `manifest daemonset` 명령으로 생성하는 것이 공식 방법입니다. 서버에 아직 컨테이너 런타임이 없으므로 **다른 머신에서 생성해 복사**하거나, 아래 템플릿을 씁니다. `vip_interface`는 서버의 실제 NIC 이름(`ip -br link`)으로 바꿉니다.

```yaml
# /var/lib/rancher/rke2/server/manifests/kube-vip.yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: kube-vip-ds
  namespace: kube-system
spec:
  selector:
    matchLabels:
      name: kube-vip-ds
  template:
    metadata:
      labels:
        name: kube-vip-ds
    spec:
      hostNetwork: true
      serviceAccountName: kube-vip
      nodeSelector:
        node-role.kubernetes.io/control-plane: "true"
      tolerations:
        - effect: NoSchedule
          operator: Exists
        - effect: NoExecute
          operator: Exists
      containers:
        - name: kube-vip
          image: ghcr.io/kube-vip/kube-vip:v1.2.4     # 2026-09-16 릴리스. 릴리스 페이지에서 최신 태그 확인
          imagePullPolicy: IfNotPresent
          args: ["manager"]
          env:
            - name: vip_arp
              value: "true"
            - name: port
              value: "6443"
            - name: vip_interface
              value: eth0
            - name: vip_cidr
              value: "32"
            - name: cp_enable
              value: "true"
            - name: cp_namespace
              value: kube-system
            - name: svc_enable
              value: "false"
            - name: vip_leaderelection
              value: "true"
            - name: vip_leaseduration
              value: "5"
            - name: vip_renewdeadline
              value: "3"
            - name: vip_retryperiod
              value: "1"
            - name: address
              value: 10.0.0.10
          securityContext:
            capabilities:
              add: ["NET_ADMIN", "NET_RAW"]
```

ARP 모드는 VIP가 한 노드에 "붙어 있는" 방식입니다. 그 노드가 죽으면 리더 선출로 다른 노드가 VIP를 가져가고, 이때 **9345를 포함한 모든 포트**가 함께 옮겨가므로 조인 주소도 VIP를 쓸 수 있습니다. 포트 단위 분산은 아니라서 API 부하 분산 효과는 없지만 3대 규모에서는 문제되지 않습니다.

```yaml
# /etc/rancher/rke2/config.yaml  (server-1)
token: ${RKE2_TOKEN}
tls-san:
  - k8s-api.example.internal
  - 10.0.0.10                              # VIP. 인증서 SAN에 없으면 VIP 접속이 TLS 오류
cni: canal
ingress-controller: traefik
write-kubeconfig-mode: "0640"
etcd-expose-metrics: true                  # Prometheus 수집용
etcd-snapshot-schedule-cron: "0 */6 * * *"
etcd-snapshot-retention: 20
etcd-s3: true
etcd-s3-endpoint: minio.example.internal:9000
etcd-s3-bucket: rke2-etcd-snapshots
etcd-s3-folder: prod
etcd-s3-access-key: ${S3_ACCESS_KEY}
etcd-s3-secret-key: ${S3_SECRET_KEY}
# etcd-s3-insecure: true                   # MinIO 가 HTTP 라면
```

`etcd-s3-config-secret` 키를 쓰면 접근 키를 config 파일 대신 Kubernetes Secret에서 읽습니다. config 파일에 키를 두면 파일 권한을 `600`으로 잠급니다.

```bash
curl -sfL https://get.rke2.io | sh -
sudo systemctl enable --now rke2-server.service
sudo journalctl -u rke2-server -f          # "rke2 is up and running" 까지 1~3분

export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
export PATH=$PATH:/var/lib/rancher/rke2/bin
kubectl get nodes                          # server-1 Ready
kubectl -n kube-system get pods | grep kube-vip   # Running
ping -c1 10.0.0.10                          # VIP 응답
```

## 3. server-2, server-3 조인

config는 server-1과 같고 `server:` 한 줄만 추가됩니다. kube-vip 매니페스트는 이미 클러스터에 배포됐으므로 다시 넣지 않습니다.

```yaml
# /etc/rancher/rke2/config.yaml  (server-2, server-3)
server: https://10.0.0.10:9345             # VIP. server-1 IP를 적으면 server-1 장애 시 재조인 실패
token: ${RKE2_TOKEN}
tls-san:
  - k8s-api.example.internal
  - 10.0.0.10
cni: canal
ingress-controller: traefik
write-kubeconfig-mode: "0640"
etcd-expose-metrics: true
etcd-snapshot-schedule-cron: "0 */6 * * *"
etcd-snapshot-retention: 20
etcd-s3: true
etcd-s3-endpoint: minio.example.internal:9000
etcd-s3-bucket: rke2-etcd-snapshots
etcd-s3-folder: prod
etcd-s3-access-key: ${S3_ACCESS_KEY}
etcd-s3-secret-key: ${S3_SECRET_KEY}
```

```bash
curl -sfL https://get.rke2.io | sh -
sudo systemctl enable --now rke2-server.service
```

**한 대씩** 올립니다. server-2가 Ready가 된 뒤 server-3을 시작합니다. 두 대를 동시에 조인하면 etcd 멤버 추가가 충돌할 수 있습니다.

server-1에서 확인합니다.

```bash
kubectl get nodes -o wide                                   # 3대 Ready, ROLES 에 control-plane,etcd,master
kubectl -n kube-system get pods -l component=etcd            # etcd-server-1/2/3 Running
kubectl -n kube-system exec etcd-server-1 -- etcdctl \
  --cacert=/var/lib/rancher/rke2/server/tls/etcd/server-ca.crt \
  --cert=/var/lib/rancher/rke2/server/tls/etcd/server-client.crt \
  --key=/var/lib/rancher/rke2/server/tls/etcd/server-client.key \
  endpoint status --cluster -w table                        # 멤버 3, IS LEADER 하나만 true
```

운영 PC의 kubeconfig는 `server:`를 VIP 이름으로 바꿔 씁니다.

```bash
scp server-1:/etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml
sed -i 's|https://127.0.0.1:6443|https://k8s-api.example.internal:6443|' ~/.kube/rke2.yaml
export KUBECONFIG=~/.kube/rke2.yaml
kubectl get nodes
```

## 4. 외부 트래픽: MetalLB + Traefik

RKE2에는 ServiceLB가 없어 Traefik의 LoadBalancer 서비스가 `<pending>`에 멈춥니다. MetalLB를 HelmChart 매니페스트로 넣습니다. 클러스터 리소스이므로 **server-1 한 곳**에만 둡니다.

```yaml
# /var/lib/rancher/rke2/server/manifests/metallb.yaml
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: metallb
  namespace: kube-system
spec:
  repo: https://metallb.github.io/metallb
  chart: metallb
  targetNamespace: metallb-system
  createNamespace: true
---
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: ingress-pool
  namespace: metallb-system
spec:
  addresses:
    - 10.0.0.20-10.0.0.29               # 서비스 VIP 대역. API VIP·노드 IP·DHCP 범위와 겹치지 않게
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: ingress-l2
  namespace: metallb-system
```

`IPAddressPool` CRD는 차트가 설치된 뒤에 생기므로 첫 적용에서 "no matches for kind" 오류가 로그에 잠깐 보입니다. helm-controller가 재시도해 곧 적용됩니다.

```bash
kubectl -n metallb-system get pods                         # controller 1, speaker 3
kubectl -n kube-system get svc rke2-traefik                # EXTERNAL-IP 10.0.0.20
curl -sI http://10.0.0.20/                                  # 404 (Traefik 응답)
```

DNS에 `*.apps.example.internal → 10.0.0.20`을 등록하면 Ingress 호스트명이 바로 동작합니다. 워크로드의 Ingress에는 `ingressClassName: traefik`을 항상 명시합니다([09 외부 트래픽 진입점](09-external-traffic-entrypoint.md)).

## 5. 스토리지: Longhorn

```yaml
# /var/lib/rancher/rke2/server/manifests/longhorn.yaml  (server-1)
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: longhorn
  namespace: kube-system
spec:
  repo: https://charts.longhorn.io
  chart: longhorn
  targetNamespace: longhorn-system
  createNamespace: true
  valuesContent: |-
    defaultSettings:
      defaultDataPath: /data/longhorn
      defaultReplicaCount: 3
    persistence:
      defaultClass: true
      defaultClassReplicaCount: 3
```

```bash
kubectl -n longhorn-system get pods                        # 전부 Running 까지 2~3분
kubectl get storageclass                                   # longhorn (default)
```

검증은 PVC 하나로 합니다.

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: pvc-test }
spec:
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 1Gi } }
---
apiVersion: v1
kind: Pod
metadata: { name: pvc-test }
spec:
  containers:
    - name: app
      image: busybox
      command: ["sh", "-c", "date >> /data/log; sleep 3600"]
      volumeMounts: [{ name: data, mountPath: /data }]
  volumes:
    - name: data
      persistentVolumeClaim: { claimName: pvc-test }
EOF
kubectl get pvc pvc-test                                   # Bound
kubectl get pod pvc-test -o wide                           # 어느 노드인지 확인
```

그 노드를 끄고(6장) Pod가 다른 노드에서 `/data/log`를 그대로 들고 다시 뜨는지 봅니다. Longhorn UI는 `kubectl -n longhorn-system port-forward svc/longhorn-frontend 8080:80`으로 봅니다. 운영에서는 Ingress + 인증을 붙여 노출합니다.

## 6. 겸임 구성의 안전장치

taint를 걸지 않으므로 앱이 컨트롤 플레인 자원을 잠식하지 못하게 합니다.

- 모든 워크로드에 `resources.requests`·`limits`를 지정합니다. 네임스페이스마다 `LimitRange`로 기본값을 두면 누락을 막습니다.
- 워크로드 requests 합계를 노드 메모리의 **70% 이내**로 유지합니다. 컨트롤 플레인 약 2GB, Longhorn 약 1GB, Canal·kube-vip·MetalLB 약 0.5GB를 먼저 빼고 계산합니다.
- 시스템 컴포넌트는 `system-cluster-critical` 우선순위라 메모리 압박 시 앱이 먼저 축출됩니다. 이 동작을 믿고 OOM까지 가게 두지 말고, `kubectl top nodes`를 Prometheus 알람으로 겁니다([Kafka 모니터링](../../kafka/concepts/14-monitoring-prometheus-grafana.md)과 같은 스택).

## 7. 장애 리허설 — 파일럿 전에 반드시

| 시나리오 | 방법 | 확인 |
| --- | --- | --- |
| VIP 없는 노드 장애 | server-2 강제 종료 | `kubectl get nodes`가 계속 응답. 5분 뒤 server-2 NotReady, 그 위 Pod가 다른 노드로 재스케줄. PVC Pod가 볼륨을 다시 붙임 |
| VIP 보유 노드 장애 | `kubectl -n kube-system logs -l name=kube-vip-ds`로 리더 확인 후 그 노드 종료 | VIP가 수 초 안에 이동. kubectl 잠깐 끊겼다 복구 |
| 2대 동시 장애 | server-2·3 종료 | API 정지. **정상 동작**이며 3대 구성의 한계. 복구는 노드를 다시 켜는 것 |
| 스냅샷 복원 | 아래 절차 | 삭제한 리소스가 돌아오는지 |
| 업그레이드 | 아래 절차 | 한 대씩, 무중단 |

### 스냅샷 복원

```bash
# 모든 server 정지
sudo systemctl stop rke2-server              # 3대 모두

# server-1 에서 S3 스냅샷으로 복원 (파일명만 지정)
sudo rke2 server --cluster-reset \
  --cluster-reset-restore-path=etcd-snapshot-server-1-1758400000 \
  --etcd-s3 --etcd-s3-endpoint=minio.example.internal:9000 \
  --etcd-s3-bucket=rke2-etcd-snapshots --etcd-s3-folder=prod \
  --etcd-s3-access-key=${S3_ACCESS_KEY} --etcd-s3-secret-key=${S3_SECRET_KEY}
sudo systemctl start rke2-server

# server-2, server-3: etcd 데이터 삭제 후 재조인
sudo rm -rf /var/lib/rancher/rke2/server/db
sudo systemctl start rke2-server
```

스냅샷 목록은 `rke2 etcd-snapshot list`로 봅니다. 복원 리허설은 **실제 데이터가 없을 때** 한 번 해 두어야 장애 때 절차를 처음 읽는 일이 없습니다.

### 업그레이드

```bash
# 한 대씩. 다음 대로 넘어가기 전에 Ready 확인
sudo curl -sfL https://get.rke2.io | INSTALL_RKE2_VERSION=v1.36.4+rke2r1 sh -
sudo systemctl restart rke2-server
kubectl get nodes                            # VERSION 갱신, Ready
```

마이너 버전은 한 단계씩만 올립니다(1.35 → 1.36). 두 단계를 건너뛰면 etcd·API 호환이 깨질 수 있습니다.

## 8. CIS 프로파일 활성화 (파일럿 후)

파일럿 워크로드가 `securityContext`(non-root, capabilities drop, seccomp)를 갖추고 나면 켭니다. [14](14-rke2-distribution.md) 3장의 사전 작업 3가지를 3대 모두에서 끝낸 뒤:

```yaml
# /etc/rancher/rke2/config.yaml 에 추가 (3대)
profile: cis
```

```bash
sudo systemctl restart rke2-server           # 한 대씩
kubectl get pods -A | grep -v Running        # 거부·재시작된 것 확인
```

거부되는 워크로드는 [12 접근 제어와 Pod 보안](12-access-control-and-pod-security.md)의 `restricted` 기준으로 고칩니다. 예외가 꼭 필요한 네임스페이스만 `pod-security.kubernetes.io/enforce: baseline` 라벨을 답니다.

## 9. 그 다음

- Prometheus + Grafana. `etcd-expose-metrics`로 etcd 지표가 2381에서 노출됩니다. kube-state-metrics, node-exporter를 함께 올립니다.
- Rancher를 이 클러스터에 설치하면 이후 클러스터·업그레이드·RBAC을 UI로 관리합니다. 관리 클러스터 분리, Custom 등록, 에이전트 동작은 [16 Rancher로 여러 클러스터 중앙 관리](16-rancher-multi-cluster-management.md)에, Helm 설치 절차는 [Lima 실습 매뉴얼](../commands/lima-rancher-rke2-lab.md) 3장에 있습니다.
- Kafka 브로커는 클러스터 밖에 그대로 두고, Connect·Schema Registry·kafka-exporter·관리 UI를 이 클러스터로 옮기는 것이 자연스럽습니다([Kafka 배치 방안](../../kafka/concepts/15-deployment-layout-options.md)).

## 관련 문서

- 왜 3대이고 왜 VIP인가: [07 실서버 클러스터 토폴로지](07-production-cluster-topology.md)
- RKE2 설정 키와 부품 설명: [14 RKE2](14-rke2-distribution.md)
- 같은 절차의 k3s 버전: [05 k3s 클러스터 설치](05-k3s-cluster-installation.md)
- Ingress·LB 구조: [09 외부 트래픽 진입점](09-external-traffic-entrypoint.md)
- 스토리지 선택 배경: [08 스토리지와 로그](08-storage-and-logging.md)

## 참고한 공식 문서

- RKE2 Documentation — Quick Start, Requirements, Backup and Restore (etcd-s3 옵션), CIS Hardening Guide (2026-09-21 확인)
- kube-vip Documentation — K3s/RKE2 usage, DaemonSet manifest generation
- MetalLB Documentation — Layer 2 configuration
- Longhorn Documentation — Installation requirements

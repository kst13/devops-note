# RKE2: 운영 배포판의 구성과 설치

이 시리즈의 실서버 기준은 [k3s](05-k3s-cluster-installation.md)입니다. 운영 환경에서 보안 감사, 표준 구성, Rancher 중앙 관리가 필요해지면 같은 회사(SUSE/Rancher)의 RKE2로 올라갑니다. 이 문서는 RKE2가 무엇인지, 최신 버전의 부품 구성, 보안 기능, 설치 절차, k3s와의 차이를 정리합니다. 기준은 2026년 8월 28일자 v1.36.4+rke2r1(Kubernetes 1.36.4)입니다.

## 1. RKE2가 무엇인가

RKE2는 SUSE의 엔터프라이즈용 Kubernetes 배포판입니다. 공식 문서는 "정부·고보안 분야를 겨냥하며, 업스트림 Kubernetes에 가깝게 유지하면서 k3s의 운영 단순함을 가져온 것"이라고 설명합니다. CNCF 인증을 통과한 완전 호환 배포판이라 워크로드 YAML은 다른 Kubernetes와 같습니다.

혈통을 알면 위치가 잡힙니다.

| 배포판 | 특징 | 관계 |
| --- | --- | --- |
| RKE1 | Docker 기반. 2025년 7월 EOL | RKE2의 전신 |
| k3s | 경량화. SQLite 기본, 레거시 제거, 엣지 지향 | RKE2가 설치·운영 방식을 물려받음 |
| **RKE2** | 업스트림 표준 부품 + CIS 강화. Docker 의존 제거, containerd 내장 | RKE1 후속, k3s 형제 |

RKE2는 컨트롤 플레인(API 서버, 스케줄러, 컨트롤러 매니저, etcd)을 **kubelet이 관리하는 static pod**로 띄웁니다. k3s가 단일 프로세스 안에서 컴포넌트를 실행하는 것과 다르고, kubeadm 방식과 같습니다. 이 점이 "업스트림에 가깝다"의 실체입니다.

## 2. 최신 구성 (v1.36 기준)

| 부품 | 기본값 | 비고 |
| --- | --- | --- |
| 데이터 저장소 | 내장 etcd 3.6 | 선택지 없음. SQLite·외부 DB 없음 |
| 컨테이너 런타임 | containerd 2.3 | Docker 의존성 없음 |
| CNI | **Canal** (Flannel + Calico) | Cilium, Calico 단독, Flannel 단독 선택 가능 |
| Ingress | **Traefik 3.7** | v1.36부터 기본. 아래 참고 |
| LoadBalancer | 없음 | MetalLB 등 별도 설치 |
| DNS·메트릭 | CoreDNS, metrics-server | |
| 멀티 NIC | Multus 옵션 | |
| Helm 차트 배포 | helm-controller 내장 | `/var/lib/rancher/rke2/server/manifests/`에 HelmChart YAML을 두면 자동 배포 |

**CNI는 클러스터 생성 전에 정해야 합니다.** 공식 문서는 "실행 중인 클러스터에서 CNI, CNI 백엔드, 클러스터·서비스 CIDR 변경을 지원하지 않는다"고 명시합니다. 듀얼 스택(IPv4/IPv6)도 생성 시점에만 설정할 수 있습니다.

### Ingress 기본값 변경 (v1.36)

Kubernetes 프로젝트의 ingress-nginx가 2026년 3월에 EOL됐습니다. RKE2는 이에 맞춰 **v1.36부터 신규 클러스터의 기본 Ingress를 Traefik으로** 바꿨습니다.

- v1.35 이하에서 만든 클러스터는 업그레이드해도 ingress-nginx가 유지됩니다. 자동으로 바뀌지 않습니다.
- ingress-nginx 차트는 더 이상 갱신되지 않으며 v1.37에서 제거됩니다. 공식 마이그레이션 가이드를 따라 Traefik으로 옮겨야 합니다.
- 에어갭 이미지 tarball(`rke2-images-core`)에도 v1.36부터 Traefik 이미지가 들어갑니다. ingress-nginx를 계속 쓰려면 별도 tarball이 필요합니다.
- 설정 키는 `ingress-controller`입니다. `traefik`(기본), `ingress-nginx`, `none` 중 하나입니다.

이 변경으로 k3s와 RKE2의 Ingress가 같아졌습니다. [02](02-ways-to-run-kubernetes.md)에서 말한 "가장자리 차이" 하나가 사라진 셈입니다.

## 3. 보안 기능 — RKE2를 고르는 이유

### CIS 프로파일

```yaml
# /etc/rancher/rke2/config.yaml
profile: cis
```

이 한 줄이 CIS Kubernetes Benchmark(v1.33 이상은 v1.12)에 맞춘 강화 설정을 켭니다.

| 적용 항목 | 내용 |
| --- | --- |
| 커널 파라미터 | CIS가 요구하는 sysctl 값 강제. 미리 안 맞추면 기동 거부 |
| etcd 유저 | etcd를 전용 `etcd` 유저·그룹으로 실행, 디렉터리 소유권 검사 |
| Pod Security Admission | 클러스터 전체에 `restricted` 강제. 시스템 네임스페이스만 예외 |
| NetworkPolicy | 시스템 네임스페이스에 "같은 네임스페이스끼리만 통신" 정책 배포. DNS·Ingress·메트릭은 예외 |
| 감사 로그 | API 서버 감사 설정 활성화. 정책 파일 `/etc/rancher/rke2/audit-policy.yaml` |

프로파일을 켜기 **전에** 운영자가 호스트에서 직접 해야 하는 일이 있습니다. 빠뜨리면 `rke2-server`가 기동하지 않습니다.

```bash
# 1) CIS sysctl 파일 배치 (설치 후 /usr/share/rke2/ 아래 제공됨)
sudo cp -f /usr/share/rke2/rke2-cis-sysctl.conf /etc/sysctl.d/60-rke2-cis.conf
sudo systemctl restart systemd-sysctl

# 2) etcd 전용 유저 생성
sudo useradd -r -c "etcd user" -s /sbin/nologin -M etcd -U

# 3) 감사 정책 작성 — 기본 파일은 아무것도 기록하지 않는 빈 정책이라 실제 규칙을 넣어야 함
sudo vi /etc/rancher/rke2/audit-policy.yaml
```

그리고 직접 만드는 네임스페이스마다 기본 ServiceAccount의 `automountServiceAccountToken: false`를 설정해야 벤치마크를 통과합니다. `restricted` PSA가 켜지므로 [12 접근 제어와 Pod 보안](12-access-control-and-pod-security.md)에서 다룬 `securityContext`(non-root, capabilities drop, seccomp)를 안 갖춘 워크로드는 배포가 거부됩니다.

### 그 외

- **FIPS 140-2** 준수 빌드 제공. 암호 모듈 검증이 필요한 규제 산업용입니다.
- 빌드 파이프라인에서 **trivy**로 CVE 스캔. 취약점이 있는 이미지는 배포되지 않습니다.
- SELinux 지원. RHEL 계열에서 `selinux: true`로 켭니다.

## 4. 요구 사양

| 항목 | 값 |
| --- | --- |
| 최소 | 2 CPU, 4GB RAM. 권장 4 CPU, 8GB |
| 컨트롤 플레인 사이징 | 2C/4G는 agent 225대까지, 4C/8G는 450대, 8C/16G는 1,300대 |
| 디스크 | etcd 때문에 **SSD 권장**. 성능이 곧 클러스터 응답성 |
| OS | SLES 16, RHEL 10, Ubuntu (SUSE 지원 매트릭스 참고). x86_64, arm64 |
| 노드 이름 | 호스트명이 클러스터 안에서 유일해야 함 |

k3s 최소 사양(server 512MB)과 비교하면 훨씬 무겁습니다. 컨트롤 플레인 컴포넌트가 각각 별도 컨테이너로 뜨기 때문입니다.

### 방화벽 포트

| 포트 | 프로토콜 | 용도 | 방향 |
| --- | --- | --- | --- |
| 6443 | TCP | Kubernetes API | agent·kubectl → server |
| **9345** | TCP | RKE2 supervisor API (노드 등록) | agent → server |
| 2379-2380 | TCP | etcd 클라이언트·피어 | server ↔ server |
| 10250 | TCP | kubelet | 전체 노드 간 |
| 8472 | UDP | Canal/Flannel VXLAN | 전체 노드 간 |
| 51820-51821 | UDP | Canal WireGuard (선택) | 전체 노드 간 |
| 4240 | TCP | Cilium 헬스체크 (Cilium 선택 시) | 전체 노드 간 |

k3s와 다른 점은 **9345**입니다. k3s는 6443 하나로 API와 노드 등록을 처리하지만, RKE2는 노드 등록용 supervisor 포트를 따로 씁니다. agent 설정의 `server:` 주소도 9345를 가리킵니다.

## 5. 설치

절차는 k3s와 거의 같습니다. 이름과 경로만 `k3s`에서 `rke2`로 바뀌고, 설정은 전부 `config.yaml`에 둡니다.

### server (첫 노드)

```bash
# 설정을 먼저 둔다. 설치 스크립트는 바이너리와 systemd 서비스만 만들고 기동은 하지 않는다.
sudo mkdir -p /etc/rancher/rke2
sudo tee /etc/rancher/rke2/config.yaml <<'EOF'
tls-san:
  - k8s-api.example.internal      # 로드밸런서·DNS 이름. 없으면 나중에 인증서 오류
cni: canal
ingress-controller: traefik
# profile: cis                    # 3장의 사전 작업을 끝낸 뒤 켠다
EOF

curl -sfL https://get.rke2.io | sh -
sudo systemctl enable --now rke2-server.service
sudo journalctl -u rke2-server -f        # "rke2 is up and running" 확인
```

설치 후 생기는 것들입니다.

| 경로 | 내용 |
| --- | --- |
| `/etc/rancher/rke2/rke2.yaml` | 관리자 kubeconfig |
| `/var/lib/rancher/rke2/server/node-token` | 노드 조인 토큰. **복구용으로 안전한 곳에 보관** |
| `/var/lib/rancher/rke2/bin/` | kubectl, crictl, ctr 바이너리. PATH에 없으므로 직접 지정 |
| `/var/lib/rancher/rke2/server/manifests/` | 여기 둔 YAML은 자동 배포 (HelmChart 포함) |
| `/var/lib/rancher/rke2/agent/etc/containerd/` | containerd 설정 |

```bash
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
export PATH=$PATH:/var/lib/rancher/rke2/bin
kubectl get nodes
```

### server 추가 (HA, 3대)

두 번째·세 번째 server는 첫 server를 가리키는 설정으로 같은 절차를 반복합니다.

```yaml
# /etc/rancher/rke2/config.yaml (server 2, 3)
server: https://server-1.example.internal:9345
token: <server-1 의 node-token>
tls-san:
  - k8s-api.example.internal
cni: canal
ingress-controller: traefik
```

etcd 쿼럼 때문에 server는 3대(또는 5대)여야 하며, 그 이유는 [07 실서버 클러스터 토폴로지](07-production-cluster-topology.md)에 있습니다. `tls-san`은 세 server 모두 같아야 하고, API 고정 접점(로드밸런서 또는 DNS)이 그 이름을 가리켜야 합니다.

### agent (워커)

```bash
sudo mkdir -p /etc/rancher/rke2
sudo tee /etc/rancher/rke2/config.yaml <<'EOF'
server: https://k8s-api.example.internal:9345
token: <node-token>
EOF

curl -sfL https://get.rke2.io | INSTALL_RKE2_TYPE="agent" sh -
sudo systemctl enable --now rke2-agent.service
```

server에서 `kubectl get nodes`로 합류를 확인합니다. agent의 `server:`는 개별 server가 아니라 **고정 접점**을 가리켜야 server 한 대가 죽어도 재연결됩니다.

### 사설 레지스트리

사내 레지스트리를 쓰면 `/etc/rancher/rke2/registries.yaml`에 미러와 인증을 적습니다. 형식은 k3s와 같습니다. [Ingress 라우팅 예제](../examples/ingress-routing/README.md)가 "RKE2에서는 이미지를 사설 레지스트리에 push"라고 한 것이 이 설정을 전제로 합니다.

```yaml
mirrors:
  docker.io:
    endpoint:
      - "https://registry.example.internal"
configs:
  "registry.example.internal":
    auth:
      username: ${REGISTRY_USER}
      password: ${REGISTRY_PASSWORD}
```

### 에어갭 설치

인터넷이 없는 서버는 릴리스 페이지의 `rke2-images-core.linux-amd64.tar.zst`를 `/var/lib/rancher/rke2/agent/images/`에 두고, 설치 스크립트 대신 바이너리 tarball을 직접 풉니다. v1.36부터 이 tarball의 Ingress 이미지는 Traefik입니다.

## 6. 운영 명령

| 작업 | 명령 |
| --- | --- |
| 서비스 상태·로그 | `systemctl status rke2-server`, `journalctl -u rke2-server -f` |
| 노드 토큰 확인 | `sudo cat /var/lib/rancher/rke2/server/node-token` |
| etcd 스냅샷 (수동) | `rke2 etcd-snapshot save --name pre-upgrade` |
| etcd 스냅샷 목록 | `rke2 etcd-snapshot list` |
| 스냅샷 복구 | `rke2 server --cluster-reset --cluster-reset-restore-path=<스냅샷>` (다른 server 정지 후) |
| 업그레이드 | 설치 스크립트를 `INSTALL_RKE2_VERSION=v1.36.4+rke2r1`로 재실행 후 `systemctl restart rke2-server`. server 먼저, agent 나중 |
| 완전 제거 | `rke2-uninstall.sh` (server), `rke2-agent-uninstall.sh` (agent) |
| 컨테이너 직접 조회 | `/var/lib/rancher/rke2/bin/crictl --runtime-endpoint unix:///run/k3s/containerd/containerd.sock ps` |

etcd 스냅샷은 기본으로 12시간마다 자동 생성되어 `/var/lib/rancher/rke2/server/db/snapshots/`에 쌓입니다. S3 업로드 옵션(`etcd-s3`)이 있으며, [minio](../../minio/README.md)를 쓰면 그 버킷을 대상으로 잡을 수 있습니다.

## 7. k3s와 비교

| | k3s | RKE2 |
| --- | --- | --- |
| 목표 | 엣지, 소규모, 리소스 제약 | 데이터센터, 규제 산업, 업스트림 정합성 |
| 컨트롤 플레인 | 단일 프로세스 | static pod (kubeadm과 같음) |
| 데이터 저장소 | SQLite 기본. etcd·외부 DB 선택 | etcd 고정 |
| CNI | Flannel | Canal (Calico 정책 엔진 포함) |
| NetworkPolicy 집행 | 내장 컨트롤러(kube-router 기반)가 집행 | Canal의 Calico가 집행. Calico 전용 정책 CRD도 사용 가능 |
| Ingress | Traefik | Traefik (v1.36부터 동일) |
| LoadBalancer | ServiceLB 내장 | 없음. MetalLB 등 별도 |
| 기본 StorageClass | local-path-provisioner | 없음. 별도 설치 |
| CIS 강화 | 수동 | `profile: cis` |
| FIPS | 없음 | 제공 |
| server 메모리 | 512MB부터 | 4GB부터 |
| 노드 등록 포트 | 6443 | 9345 (+ 6443 API) |
| 경로 | `/etc/rancher/k3s`, `/var/lib/rancher/k3s` | `/etc/rancher/rke2`, `/var/lib/rancher/rke2` |
| 설정 방식 | `config.yaml` + 설치 시 환경변수 | `config.yaml` |

Ingress가 같아지면서 실질적 차이는 **etcd 고정, Calico 정책 엔진, CIS/FIPS, 내장 LB·StorageClass 유무, 메모리**로 좁혀졌습니다.

NetworkPolicy는 **둘 다 집행합니다.** 표준 Flannel 단독 구성은 정책을 무시하지만, k3s는 kube-router 기반 컨트롤러를 내장해 표준 NetworkPolicy를 집행합니다([13 NetworkPolicy](13-network-policy.md)). RKE2의 Canal은 Calico가 집행하며, 표준 리소스 외에 Calico 전용 정책(GlobalNetworkPolicy 등)도 쓸 수 있습니다. 개발 k3s에서 검증한 표준 NetworkPolicy는 운영 RKE2에서도 같은 결과를 내므로, 정책 검증 결과가 환경 사이에서 뒤집히지는 않습니다.

## 8. 어느 쪽을 고르나

| 상황 | 선택 |
| --- | --- |
| 노드 수 적고 사내 서비스 규모, 관리 인력 적음 | k3s로 충분. 3대 HA도 내장 etcd로 가능 |
| 매장·공장·엣지, 리소스 제약 | k3s |
| 보안 감사·규제 대응(CIS, FIPS, 감사 로그) 요구 | RKE2 |
| Calico 전용 정책이나 Cilium 관측 기능이 필요 | RKE2 (또는 k3s에 Calico/Cilium 교체) |
| Rancher로 여러 클러스터 중앙 관리 | RKE2. Rancher가 프로비저닝하는 기본 배포판 |
| 나중에 EKS 등 관리형 이전 가능성 | RKE2. 부품이 표준이라 이질감 적음 |

k3s는 실습용이 아닙니다. 프로덕션 인증 배포판이고 HA·백업·업그레이드 절차를 공식 지원합니다. 결정 기준은 규모가 아니라 **보안 요건과 표준 구성 요구가 있느냐**입니다.

## 관련 문서

- 3대 실전 구축 절차 (kube-vip·MetalLB·Longhorn·백업·리허설): [15 RKE2 3대 클러스터 구축](15-rke2-three-node-build.md)
- 배포판 선택지 전체 비교: [02 쿠버네티스를 경험하는 방법들](02-ways-to-run-kubernetes.md)
- k3s 설치 (같은 절차의 k3s 버전): [05 k3s 클러스터 설치](05-k3s-cluster-installation.md)
- server 3대와 API 고정 접점: [07 실서버 클러스터 토폴로지](07-production-cluster-topology.md)
- CIS 프로파일이 강제하는 Pod 보안: [12 접근 제어와 Pod 보안](12-access-control-and-pod-security.md)
- 두 배포판 모두 집행하는 NetworkPolicy: [13 NetworkPolicy](13-network-policy.md)

## 참고한 공식 문서

- RKE2 Documentation — Introduction, Quick Start, Requirements, Basic Network Options, Networking Services, CIS Hardening Guide, Release Notes v1.36.X (2026-09-21 확인)
- RKE2 — Ingress NGINX to Traefik Migration Guide

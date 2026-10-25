# Rancher·RKE2 실서버 설치 실패 사례

2026-10-19~21에 Rocky Linux 9 실서버에 MGMT 3대(RKE2 v1.36.4 + Rancher 2.15.2)와 DEV 3대(Rancher Custom, RKE2 v1.36.5)를 설치하면서 실제로 막힌 사례입니다. 아웃바운드가 기본 차단된 사내망이라는 조건이 대부분의 원인입니다. 준비 절차는 [17 실서버 Rancher + RKE2 구축 준비](../concepts/17-onprem-rancher-rke2-preparation.md)에 있습니다.

| 사례 | 단계 | 한 줄 요약 |
| --- | --- | --- |
| 1 | RKE2 설치 | 차단된 EPEL·`rpm.rancher.io` 때문에 `dnf` 전체가 실패 |
| 2 | RKE2 기동 | 붙여 넣기로 YAML 들여쓰기가 깨짐 |
| 3 | MGMT 조인 | `node-name` 누락으로 OS hostname이 노드 이름이 됨 |
| 4 | Rancher 설치 | Docker Hub CDN이 막혀 큰 이미지만 실패 |
| 5 | Rancher 설치 | `helm template`이 Kubernetes 1.37로 검사 |
| 6 | Rancher 설치 | Pod는 떴는데 Helm 기록만 실패 |
| 7 | DEV 외부 접속 | RKE2 Traefik 차트의 Service 키가 다름 |
| 8 | 앱 운영 | Rancher UI Scale과 Helm이 같은 필드를 다툼 |

## 1. 설치 스크립트가 멈추고 SSH가 끊김

### 증상

설치 스크립트가 `using 1.36 series from channel stable`을 출력한 뒤 아래 메시지를 남기고 멈춥니다. 이어서 `logout`이 나오며 SSH 접속이 끊깁니다.

```text
Curl error (35): SSL connect error ... Connection reset by peer
Failed to download metadata for repo 'epel'
```

### 원인

- `dnf`는 설치 전에 켜진 저장소의 메타데이터를 모두 받습니다.
- 방화벽이 `mirrors.fedoraproject.org`(EPEL)나 `rpm.rancher.io`를 막으면 TLS 연결이 끊기고 `dnf` 전체가 실패합니다.
- EPEL은 RKE2 설치에 쓰지 않지만, 켜져 있으면 함께 실패를 일으킵니다.
- SSH가 끊긴 이유는 명령 끝의 `|| exit 1`이 실패 시 현재 셸을 종료했기 때문입니다.

### 확인

```bash
curl -sSI https://rpm.rancher.io        # Connection reset by peer 면 방화벽 차단
dnf repolist                             # epel, rancher-rke2-* 가 켜져 있는지
```

### 해결

- EPEL은 끕니다: `sudo dnf config-manager --set-disabled epel`.
- 실패한 실행이 추가한 `rancher-rke2-*` 저장소도 끕니다.
- `rpm.rancher.io` 허용을 신청하거나, PC에서 tar 묶음을 받아 반입하는 방식으로 설치합니다. tar 방식은 17번 문서 5장에 있습니다.

## 2. Traefik 설정 파일 ParseManifestFailed

### 증상

`rke2-server` 기동 로그에 다음이 나옵니다. Traefik은 설정 없이 기본값으로 뜹니다.

```text
ParseManifestFailed ... yaml: line 2: mapping values are not allowed in this context
Failed to process config: failed to process .../rke2-traefik-config.yaml
```

### 원인

붙여 넣은 터미널이 줄 앞에 들여쓰기를 덧붙여 YAML 구조가 깨졌습니다. 줄이 합쳐지거나 CRLF가 섞여도 같은 오류가 납니다.

### 확인

```bash
# 줄 끝은 $로 표시됨. 줄 앞 공백, ^M(CRLF), ^I(탭)가 없어야 함
sudo cat -A /var/lib/rancher/rke2/server/manifests/rke2-traefik-config.yaml | head -5
```

1~2번째 줄이 `apiVersion: helm.cattle.io/v1$`, `kind: HelmChartConfig$`처럼 공백 없이 시작해야 합니다.

### 해결

`tee` 블록 전체를 끝 표시까지 한 번에 다시 붙여 넣습니다. 들여쓰기가 계속 덧붙으면 첫 줄의 공백 수만큼 모든 줄에서 지웁니다.

```bash
f=/var/lib/rancher/rke2/server/manifests/rke2-traefik-config.yaml
n=$(sudo head -1 "$f" | sed -E 's/^( *).*/\1/' | wc -c); n=$((n-1))
sudo sed -i -E "s/^ {0,$n}//; s/\r$//" "$f"
```

RKE2가 `manifests/` 폴더를 감시하므로 재시작은 필요 없습니다. 로그에 `Applied manifest ... rke2-traefik-config.yaml`이 나오면 다시 읽은 것입니다.

## 3. 노드가 hostname으로 조인됨

### 증상

`kubectl get nodes`에 MGMT-2가 `mgmt-2`가 아니라 OS hostname으로 보입니다. config.yaml을 고치고 재시작해도 이름이 바뀌지 않습니다.

### 원인

- MGMT-1의 config.yaml을 복사하면서 `node-name`, `node-ip` 줄을 뺐습니다.
- 노드 이름은 etcd 멤버 이름과 kubelet 인증서에 들어갑니다. 한 번 조인한 뒤에는 config로 바꿀 수 없습니다.

### 확인

```bash
kubectl get nodes -o wide
sudo grep -nE 'node-name|node-ip|server:' /etc/rancher/rke2/config.yaml
```

### 해결

노드를 지우고 다시 조인합니다. **etcd가 3개일 때만 한 대씩** 합니다. 2개일 때 한 대를 빼면 과반이 깨져 API가 멈춥니다.

```bash
# MGMT-1: 잘못된 노드 삭제. etcd 멤버도 함께 빠짐
kubectl delete node <잘못된 이름>

# 해당 노드: 중지 후 server 데이터와 노드 인증서만 삭제
sudo systemctl stop rke2-server
sudo /usr/local/bin/rke2-killall.sh
sudo rm -rf /var/lib/rancher/rke2/server /etc/rancher/node
# config.yaml 에 node-name, node-ip 추가 후
sudo systemctl start rke2-server
```

`/var/lib/rancher/rke2/agent/images/`의 이미지 묶음과 `/etc/rancher/rke2/`의 config·토큰 파일은 지우지 않습니다.

config를 복사할 때 `token:` 줄을 주석으로 남겨 두면, 화면이나 채팅에 붙일 때 토큰이 노출됩니다. 토큰은 `token-file`로만 전달하고, 노출됐다면 클러스터 완성 후 `rke2 token rotate`로 교체합니다.

## 4. 큰 이미지만 ImagePullBackOff

### 증상

- `helm install rancher`가 15분 뒤 실패하고 Rancher Pod가 `ImagePullBackOff`에 머뭅니다.
- 같은 시점에 `rancher/shell`(114MB)은 받아지고 `rancher/rancher`(973MB)만 실패합니다.
- 이벤트에 `connection reset by peer` 또는 `timeout awaiting response headers`가 보입니다.

### 원인

- Docker Hub는 이미지 목록을 `registry-1.docker.io`에서 주고, 레이어 데이터는 CDN에서 줍니다.
- 1차 방화벽 신청에는 `production.cloudflare.docker.com`만 있었습니다.
- 실제로는 `production.cloudfront.docker.com`(AWS CloudFront)에서도 받았고, 이 주소가 막혀 있었습니다.
- 작은 이미지는 다른 CDN 경로로 받아져, 방화벽이 열린 것처럼 보였습니다.

### 확인

```bash
kubectl -n cattle-system describe pod -l app=rancher | grep -A5 Events
# 받는 중인지 멈췄는지: 임시 파일 크기가 늘면 받는 중
D=/var/lib/rancher/rke2/agent/containerd/io.containerd.content.v1.content/ingest
sudo du -sh $D; sleep 30; sudo du -sh $D
```

### 해결

- 근본 해결은 `production.cloudfront.docker.com` 허용입니다. IP가 수시로 바뀌므로 FQDN 허용과 TLS 검사 예외를 함께 요청합니다.
- 방화벽이 열리기 전에는 이미 받은 노드에서 이미지를 옮깁니다.

```bash
ctr() { sudo /var/lib/rancher/rke2/bin/ctr -a /run/k3s/containerd/containerd.sock -n k8s.io "$@"; }
# 받은 노드
ctr images export --platform linux/amd64 ~/rancher.tar docker.io/rancher/rancher:v2.15.2
sudo chown "$USER" ~/rancher.tar
scp ~/rancher.tar <계정>@<대상 노드>:~/
# 받을 노드
ctr images import --platform linux/amd64 ~/rancher.tar && rm -f ~/rancher.tar
kubectl -n cattle-system delete pod <ImagePullBackOff Pod>
```

- `content digest ... not found`가 나면 `--platform linux/amd64`가 빠졌는지 봅니다.
- DEV의 `cattle-cluster-agent` 이미지(`rancher/rancher-agent`)는 MGMT에 없습니다. 먼저 받은 DEV 노드에서 옮깁니다.

## 5. helm template kubeVersion 오류

### 증상

```text
chart requires kubeVersion: < 1.37.0-0 which is incompatible with Kubernetes v1.37.0
```

### 원인

- `helm template`은 클러스터에 접속하지 않습니다. 그래서 Helm 3.22의 기본값인 Kubernetes 1.37로 검사합니다.
- 실제 클러스터(1.36.4)는 조건을 만족합니다. `helm install`은 실제 버전으로 검사하므로 영향이 없습니다.

### 확인

```bash
kubectl version | grep Server
helm version
```

### 해결

`helm template ... --kube-version 1.36.4`를 붙입니다. 이 오류는 Rancher 2.15.2가 Kubernetes 1.37을 지원하지 않는다는 뜻이기도 합니다. Rancher를 올리기 전에 RKE2를 1.37로 올리지 않습니다.

## 6. Rancher release가 failed로 남음

### 증상

- Rancher Pod 3개가 노드별로 `Running`인데 `helm list -n cattle-system -a`는 `failed`입니다.
- 또는 `helm upgrade`가 `pre-upgrade hooks failed: timed out waiting for the condition`으로 실패합니다.
- 늦게 실행된 hook Pod 로그에 `the server has asked for the client to provide credentials`가 남습니다.

### 원인

- 이미지 다운로드가 Helm timeout(15분)보다 오래 걸렸습니다. Helm은 실패로 기록했지만, 이미지가 받아진 뒤 Pod는 정상으로 떴습니다.
- upgrade 전에 도는 `rancher-pre-upgrade` Job도 `rancher/shell` 이미지를 기다리다 timeout됐습니다.
- Helm은 포기하면서 hook용 ServiceAccount를 정리합니다. 그 뒤에 실행된 Pod는 무효가 된 토큰으로 API를 호출해 실패했습니다.

### 확인

```bash
helm list -n cattle-system -a
kubectl -n cattle-system get jobs,pods
```

### 해결

- `helm uninstall` 하지 않습니다. Rancher Pod는 뜨자마자 CRD와 `cattle-*` 네임스페이스를 만들기 때문에, uninstall하면 그 리소스가 남은 채 Rancher만 지워집니다.
- 남은 hook Job을 지운 뒤 같은 values로 `helm upgrade`를 다시 실행합니다.

```bash
kubectl -n cattle-system delete job rancher-pre-upgrade --ignore-not-found
helm upgrade rancher <차트.tgz> -n cattle-system -f values.yaml --wait --timeout 15m
```

values가 같으면 Pod를 다시 만들지 않고 hook만 돌고 `deployed`가 됩니다. 다음 upgrade의 비교 기준을 맞추기 위해 꼭 실행합니다.

## 7. Traefik Service가 LoadBalancer로 바뀌지 않음

### 증상

- DEV 클러스터의 Additional Manifest에 Traefik `HelmChartConfig`를 넣었는데 `helm-install-rke2-traefik` Job이 계속 `Error`입니다.
- 그동안 Traefik Service와 Pod가 사라집니다.
- Job 로그 마지막 줄은 다음과 같습니다.

```text
spec.externalTrafficPolicy: Invalid value: "Local": may only be set for externally-accessible services
```

### 원인

- `service.type: LoadBalancer`로 적었습니다.
- RKE2 Traefik 차트는 Service 타입을 `service.spec.type`에서 읽습니다. 그래서 `service.type`은 무시되고 기본값 ClusterIP가 남았습니다.
- ClusterIP에 `externalTrafficPolicy: Local`이 들어가 검증에 실패했습니다. Job은 실패한 릴리스를 지우고 다시 설치하므로 그동안 Traefik이 없어집니다.

### 확인

차트의 실제 키를 차트에서 직접 봅니다.

```bash
kubectl -n kube-system get helmchart rke2-traefik -o jsonpath='{.spec.chartContent}' | base64 -d > ~/rke2-traefik.tgz
tar -xOzf ~/rke2-traefik.tgz rke2-traefik/values.yaml | grep -n -A25 '^service:'
```

### 해결

`service.spec.type`과 `service.spec.externalTrafficPolicy`로 고칩니다. 전체 설정은 [18 DEV 클러스터 외부 접속](../concepts/18-dev-cluster-metallb-traefik-tls.md)에 있습니다.

## 8. Rancher 화면에서 Scale한 뒤 Helm Upgrade 실패

### 증상

Rancher Apps로 설치한 앱을 Upgrade하면 실패합니다.

```text
Upgrade "<앱>" failed: conflict with "agent" using apps/v1: .spec.replicas
```

### 원인

- Rancher 워크로드 화면의 +/−(Scale)를 쓰면 `spec.replicas` 필드의 주인이 Rancher(`agent`)로 기록됩니다.
- 다음 Helm Upgrade가 같은 필드를 쓰려다 충돌합니다.

### 확인

```bash
kubectl -n <ns> get deploy <앱> --show-managed-fields -o yaml | grep -B2 -A3 'f:replicas'
```

### 해결

- 그 Deployment를 지우고 Installed Apps에서 같은 버전으로 다시 Upgrade합니다. Helm이 새로 만들며 주인이 Helm으로 돌아옵니다. 그동안 앱은 잠시 내려갑니다.
- 이후 Pod 수는 차트 values(`replicas`)를 바꿔 Upgrade로만 조정합니다. 잠시 멈출 때도 `replicas: 0`으로 Upgrade합니다.

## 관련 문서

- 준비 절차와 방화벽 목록: [17 실서버 Rancher + RKE2 구축 준비](../concepts/17-onprem-rancher-rke2-preparation.md)
- DEV의 MetalLB, Traefik, 인증서: [18 DEV 클러스터 외부 접속](../concepts/18-dev-cluster-metallb-traefik-tls.md)
- 로컬 실습에서 막힌 지점: [Lima + Rancher + RKE2 실습 매뉴얼](../commands/lima-rancher-rke2-lab.md) 10장

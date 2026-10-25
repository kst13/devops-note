# DEV 클러스터 외부 접속: MetalLB VIP, Traefik, 와일드카드 인증서

Rancher로 만든 DEV 클러스터에 사용자 요청이 들어오는 경로를 만듭니다. 관리 클러스터는 인프라팀 L4가 앞에 있지만, DEV에는 L4가 없습니다. 그래서 **MetalLB로 VIP 하나를 만들고 Traefik에만 붙입니다.** 2026-10-21에 실서버 DEV 3대(RKE2 v1.36.5)에 적용한 구성입니다. 준비 단계는 [17 실서버 Rancher + RKE2 구축 준비](17-onprem-rancher-rke2-preparation.md)에 있습니다.

```text
사용자 ─▶ <앱>.dev.example.internal = DEV VIP 10.0.1.20
          │  MetalLB L2: DEV 노드 한 대가 ARP로 VIP에 응답. 죽으면 다른 노드가 이어받음
          ▼
        Traefik (RKE2 기본 Ingress, DEV 3대에 하나씩)   TLS 처리, Host 헤더로 앱 구분
          ▼
        앱별 Service → Pod
```

## 1. 구성 요소와 선택 이유

| 구성 요소 | 역할 | 선택 이유 |
| --- | --- | --- |
| MetalLB (L2 모드) | 클러스터 안에서 L4 장비 역할. 같은 대역의 빈 IP를 VIP로 쓰고 ARP로 알림 | L4 장비 없이 `LoadBalancer` Service에 IP를 줄 수 있음 |
| Traefik | TLS 종료, 도메인별 라우팅 | RKE2 1.36 신규 설치의 기본 Ingress. 별도 설치 불필요 |
| 와일드카드 TLS Secret | Traefik 기본 인증서 | 앱 Ingress마다 인증서를 적지 않아도 됨 |

- **VIP는 Traefik에만 줍니다.** 모든 앱 도메인이 이 VIP를 같이 씁니다. 앱이 늘어도 VIP를 더 받지 않습니다.
- **처리량은 노드 한 대분입니다.** L2 모드에서는 모든 요청이 VIP를 가진 노드로 먼저 들어옵니다. 노드가 갑자기 죽으면 넘겨받기까지 몇 초 끊깁니다. DEV에는 충분합니다.

## 2. 인프라팀에 받을 것

| 항목 | 내용 |
| --- | --- |
| IP | DEV 노드와 같은 대역의 빈 IP 1개. 다른 장비가 쓰지 않게 예약 |
| 스위치 | VIP의 ARP 응답 MAC이 바뀌어도 막지 않을 것 (Port security, Dynamic ARP Inspection) |
| DNS | 앱 도메인 → VIP. 와일드카드(`*.dev.example.internal`)로 받으면 앱을 추가할 때 다시 요청하지 않음 |
| 방화벽 | 사용자 PC 대역 → VIP 80/443 |

받은 뒤 DEV 노드에서 확인합니다.

```bash
ip -br a                                       # NIC 이름 확인
sudo arping -I <NIC> -c 3 10.0.1.20            # Received 0 response(s) 여야 빈 IP
getent hosts app.dev.example.internal          # VIP가 나와야 함
kubectl get ingressclass                       # traefik (default)
kubectl -n kube-system get svc rke2-traefik    # 설치 직후에는 ClusterIP
```

## 3. MetalLB 설치

MetalLB 이미지는 quay.io에 있습니다. quay.io는 레이어를 CDN(`cdn01.quay.io` 등)에서 주는데, 사내망에서는 이 단계가 막히기 쉽습니다. 방화벽이 열리기 전에는 Jenkins 서버나 PC에서 이미지를 받아 Nexus에 올리고 매니페스트의 이미지 주소를 바꿉니다.

```bash
# PC 또는 Jenkins 서버: 이미지를 Nexus 로 옮김 (speaker 도 같은 방법)
docker pull --platform linux/amd64 quay.io/metallb/controller:v0.15.2
docker tag quay.io/metallb/controller:v0.15.2 nexus.example.internal:5000/metallb/controller:v0.15.2
docker push nexus.example.internal:5000/metallb/controller:v0.15.2

# PC: 매니페스트를 받아 이미지 주소 교체 후 노드로 복사
curl -fsSLO https://raw.githubusercontent.com/metallb/metallb/v0.15.2/config/manifests/metallb-native.yaml
sed -i '' 's#quay.io/metallb/#nexus.example.internal:5000/metallb/#g' metallb-native.yaml   # macOS sed
grep 'image:' metallb-native.yaml
scp metallb-native.yaml <계정>@<DEV-1>:~/

# DEV-1
kubectl apply -f ~/metallb-native.yaml
kubectl -n metallb-system get pods -o wide -w   # controller 1, speaker 3 Running
```

speaker Pod IP가 노드 IP와 같은 것은 정상입니다. ARP로 응답하려고 hostNetwork로 뜹니다.

VIP를 등록합니다.

```yaml
# metallb-pool.yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: dev-ingress
  namespace: metallb-system
spec:
  addresses:
    - 10.0.1.20/32
  autoAssign: false        # 지정한 Service(Traefik)만 가져가게
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: dev-ingress
  namespace: metallb-system
spec:
  ipAddressPools:
    - dev-ingress
  interfaces:
    - <NIC>                # 실제 NIC에서만 ARP 응답
```

```bash
kubectl apply -f ~/metallb-pool.yaml       # webhook 오류면 30초 뒤 다시
```

`autoAssign: false`가 중요합니다. 없으면 나중에 누군가 만든 `LoadBalancer` Service가 VIP를 먼저 가져갈 수 있습니다.

## 4. Traefik Service를 LoadBalancer로

Rancher가 만든 클러스터는 노드에 직접 `kubectl apply`한 기본 차트 설정을 Rancher가 덮어쓸 수 있습니다. 그래서 Rancher 화면의 `dev` ⋮ → **Edit Config** → **Additional Manifest**에 넣습니다. Traefik은 Add-on 탭이 따로 없습니다.

```yaml
apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: rke2-traefik
  namespace: kube-system
spec:
  valuesContent: |-
    service:
      annotations:
        metallb.io/loadBalancerIPs: 10.0.1.20
      spec:
        type: LoadBalancer
        externalTrafficPolicy: Local    # 사용자 IP 유지
    tlsStore:
      default:
        defaultCertificate:
          secretName: wildcard-tls      # 5장에서 만듦
```

- **`HelmChartConfig`**는 RKE2 기본 차트에 values를 덧붙이는 리소스입니다. 이름과 네임스페이스가 차트와 같아야 합니다.
- **`service.spec.type`으로 씁니다.** RKE2 Traefik 차트는 기본값이 `service.spec.type: ClusterIP`입니다. 일반 Traefik 차트처럼 `service.type`을 쓰면 무시되고, 실패 사례는 [트러블슈팅 7](../troubleshooting/rancher-rke2-onprem-install-failures.md)에 있습니다.
- **`externalTrafficPolicy: Local`**은 사용자 IP를 Pod까지 남깁니다. Local은 그 노드에 Pod가 있어야 트래픽을 받는데, Traefik이 3대 모두에 있어 손해가 없습니다.
- Additional Manifest 한 칸에 여러 리소스를 넣을 때는 `---`로 이어 붙입니다. 기존 내용을 지우면 그 리소스 설정도 빠집니다.

Save하면 `helm-install-rke2-traefik` Job이 다시 돕니다.

```bash
kubectl -n kube-system get pods | grep helm-install-rke2-traefik   # Completed
kubectl -n kube-system get svc rke2-traefik     # LoadBalancer, EXTERNAL-IP 10.0.1.20
```

PC에서 `curl -s -o /dev/null -w '%{http_code}\n' http://app.dev.example.internal/`가 `404`면 연결된 것입니다. Ingress가 아직 없어 Traefik이 404로 답합니다.

## 5. 와일드카드 인증서를 Traefik 기본 인증서로

인증서와 개인키는 기존 서버에서 DEV 노드로 `scp`합니다. 채팅이나 git에 올리지 않습니다.

```bash
grep -c 'BEGIN CERTIFICATE' ~/cert.pem              # 2 이상이면 중간 인증서 포함
openssl x509 -in ~/cert.pem -noout -subject -enddate
openssl x509 -in ~/cert.pem -noout -pubkey | sha256sum
openssl pkey -in ~/key.pem -pubout | sha256sum      # 위와 같아야 짝

kubectl -n kube-system create secret tls wildcard-tls --cert=$HOME/cert.pem --key=$HOME/key.pem
kubectl -n kube-system get secret wildcard-tls      # TYPE kubernetes.io/tls, DATA 2
rm ~/key.pem
```

- **Secret은 `kubectl`로 만듭니다.** Additional Manifest에 넣으면 개인키가 Rancher 클러스터 설정에 평문으로 남습니다.
- **`kube-system`에 만듭니다.** Traefik 기본 인증서는 Traefik과 같은 네임스페이스의 Secret만 쓸 수 있습니다.
- **개인키는 `openssl pkey`로 읽습니다.** 개인키 파일에 `openssl x509`를 쓰면 출력이 비어 해시가 늘 `e3b0c442...`(빈 입력의 sha256)로 나옵니다. 짝이 맞는 것처럼 보이는 실수입니다.
- **갱신**은 Secret만 새 파일로 바꿉니다. Traefik이 다시 읽으므로 재시작이 필요 없습니다.

```bash
kubectl -n kube-system create secret tls wildcard-tls --cert=new.crt --key=new.key \
  --dry-run=client -o yaml | kubectl apply -f -
```

PC에서 `-k` 없이 `curl -s -o /dev/null -w '%{http_code}\n' https://app.dev.example.internal/`가 `404`면 끝입니다. 인증서 등록 전에는 Traefik 자체서명 인증서라 `-k` 없이는 `000`이 나옵니다.

### 두 번째 도메인 추가

다른 도메인(`*.dev.example2.internal`)의 앱도 같은 VIP로 받을 수 있습니다.

- **기본 인증서는 하나뿐입니다.** 추가 인증서는 같은 TLSStore의 `certificates` 목록에 넣습니다.
- **Traefik은 요청 도메인(SNI)과 맞는 인증서를 고릅니다.** 맞는 것이 없으면 기본 인증서를 냅니다.

```yaml
    tlsStore:
      default:
        defaultCertificate:
          secretName: wildcard-tls
        certificates:
          - secretName: wildcard2-tls
```

```bash
# DNS 가 아직 없으면 --resolve 로 VIP 를 직접 지정
curl -s -o /dev/null -w '%{http_code}\n' --resolve app.dev.example2.internal:443:10.0.1.20 https://app.dev.example2.internal/
echo | openssl s_client -connect 10.0.1.20:443 -servername app.dev.example2.internal 2>/dev/null \
  | openssl x509 -noout -subject            # subject 가 *.dev.example2.internal
```

앱 차트의 Ingress에는 TLS Secret 이름을 적지 않습니다. 비워 두면 Traefik이 TLSStore에서 고릅니다.

## 6. 남은 작업과 운영 메모

- **VIP 넘기기 시험:** VIP를 가진 노드를 재부팅해 다른 노드가 이어받는지 봅니다. 스위치의 ARP 정책 확인을 겸합니다. 현재 VIP를 가진 노드는 `kubectl -n metallb-system logs ds/speaker --all-containers | grep 10.0.1.20`으로 찾습니다.
- **80→443 리다이렉트와 긴 요청 시간 제한:** 기존 proxy nginx가 하던 일입니다. 같은 HelmChartConfig의 `ports.web` 아래에 넣습니다. 키는 4장의 차트 `values.yaml`에서 확인합니다.
- **설정은 Rancher에서 바꿉니다.** 노드의 `50-rancher.yaml`, `registries.yaml`은 Rancher가 덮어씁니다.
- **클러스터 밖으로 나가는 요청의 출발지가 DEV 노드 3대입니다.** 화면은 뜨는데 API가 502면 레거시 서버의 방화벽부터 봅니다.

## 관련 문서

- 실서버 준비와 방화벽: [17 실서버 Rancher + RKE2 구축 준비](17-onprem-rancher-rke2-preparation.md)
- 설치 중 막힌 사례: [Rancher·RKE2 실서버 설치 실패 사례](../troubleshooting/rancher-rke2-onprem-install-failures.md)
- Ingress와 L4의 역할 분담: [09 외부 트래픽 진입점](09-external-traffic-entrypoint.md)
- web/WAS 차트를 Rancher Apps로 배포: [web/WAS 릴리즈 예제](../examples/web-was-release/README.md)

## 참고한 공식 문서

- MetalLB Documentation — Installation by manifest, Layer 2 configuration, IPAddressPool `autoAssign` (v0.15 기준)
- RKE2 Documentation — Networking Services(Traefik), Helm Integration(`HelmChartConfig`) (v1.36 기준)
- Traefik Documentation — TLS Store, default certificate (v3 기준)
- Rancher Manager Documentation — RKE2 cluster configuration(Additional Manifest)

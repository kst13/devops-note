# web/WAS 릴리즈 실습: 빌드·push는 Jenkins, 배포는 Rancher

"Jenkins는 이미지와 차트를 만들어 저장소에 쌓고, 릴리즈 실행은 Rancher UI에서 한다"는 분업을 Lima 환경(mgmt + dev1)에서 그대로 연습하는 샘플입니다. web은 nginx 정적 페이지, WAS는 DB 없는 Spring Boot API입니다. Jenkins Job은 web, was **둘**이고 각 앱 디렉터리가 자기 Dockerfile·차트·Jenkinsfile을 갖습니다. 설계 배경은 [16 Rancher로 여러 클러스터 중앙 관리](../../concepts/16-rancher-multi-cluster-management.md) 9장, 실서버 준비는 [17 실서버 Rancher + RKE2 구축 준비](../../concepts/17-onprem-rancher-rke2-preparation.md)에 있습니다.

```text
Mac (Docker Desktop)                         dev1 VM 192.168.64.3                mgmt VM 192.168.64.2
 build.sh 1.0.0                               ┌─ registry Pod :5050 ◀── push      ┌─ chartmuseum Pod :8085 ◀── 차트 push
  ├ web: docker build/push, helm package      │     ▲ pull                        │        ▲ index 읽기
  └ was: gradlew, docker build/push, package  │  web-portal ×2 ── /api/ ──▶ was-order ×2   │
                                              │     ▲                             └─ Rancher ──┘
 Safari  http://app.dev.192-168-64-3.sslip.io ─▶ Traefik ─▶ web-portal
 Safari  https://rancher.192-168-64-1.sslip.io:8443  →  Deployment 태그 변경 / Apps 업그레이드 / 롤백
```

| 경로 | 내용 | 실서버 대응 |
| --- | --- | --- |
| `web/` | nginx + index.html. `/api/`를 was-order Service로 프록시 | web 저장소 |
| `web/chart/` | web-portal 차트 (Deployment, Service, Ingress) | 같은 저장소 안 |
| `web/Jenkinsfile` | 이미지 build·push → 같은 버전으로 차트 package·push | web Job |
| `was/` | Spring Boot 3.5, `/api/hello`, `/api/orders`, actuator health | was 저장소 |
| `was/chart/` | was-order 차트 (Deployment, Service) | 같은 저장소 안 |
| `was/Jenkinsfile` | jar 빌드 → 이미지 build·push → 차트 package·push | was Job |
| `build.sh` | 위 두 Job을 로컬에서 대신 실행 | 실습 전용 |
| `lab/registry.yaml` | dev1에 올리는 이미지 레지스트리 (HTTP, 5050) | Nexus docker hosted 5000 |
| `lab/chartmuseum.yaml` | mgmt에 올리는 Helm 저장소 (8085) | Nexus helm hosted 8081 |
| `lab/registries.yaml` | dev1 노드가 HTTP 레지스트리를 쓰게 하는 설정 | Rancher UI Registries에 Nexus 주소 |
| `k8s/web-was.yaml` | 방식 A용 평문 매니페스트 (두 앱 한 파일) | 그대로 |

**버전 규칙은 하나입니다.** 앱마다 `이미지 태그 = 차트 버전 = 빌드 버전`. Jenkins에서는 `1.0.<빌드번호>`, 실습에서는 `build.sh`에 넘긴 값입니다. 차트의 `appVersion`이 이미지 태그 기본값이라 values에 태그를 따로 적을 필요가 없습니다.

레지스트리를 dev1 안에, 차트 저장소를 mgmt 안에 두는 이유는 로컬 환경의 두 제약 때문입니다. VM 사이 통신이 막혀 있어 "이미지를 받는 쪽(dev1)"과 "차트를 읽는 쪽(Rancher, mgmt)" 각각의 안에 두어야 하고, Mac 메모리가 16GB라 Docker Desktop을 VM 두 대와 오래 같이 띄울 수 없습니다. 실서버에서는 둘 다 Nexus 하나입니다.

## 0. 사전 준비

- mgmt, dev1이 Running이고 Rancher에 dev 클러스터가 Active ([Lima 실습 매뉴얼](../../commands/lima-rancher-rke2-lab.md) 7장까지 완료)
- Mac에 JDK 21, Helm, kubectl, Docker Desktop
- dev 클러스터 kubeconfig를 Mac에 합치고 context 전환 (매뉴얼 8장). 아래 `kubectl`은 모두 dev context 기준

Docker Desktop이 HTTP 레지스트리에 push하도록 허용합니다. Settings → Docker Engine에 추가 후 Apply.

```json
{
  "insecure-registries": ["192.168.64.3:5050"]
}
```

메모리가 빡빡하면 Docker Desktop 메모리를 2GB로 낮추고, 빌드·push가 끝나면 Docker Desktop을 종료해도 됩니다. 배포 단계에는 Docker가 필요 없습니다.

## 1. 레지스트리 올리기 (dev1)

```bash
kubectl apply -f lab/registry.yaml
kubectl -n tools rollout status deploy/registry
curl -s http://192.168.64.3:5050/v2/_catalog          # {"repositories":[]}
```

dev1 노드의 containerd가 이 레지스트리를 HTTP로 쓰게 합니다. 실서버에서 Nexus를 등록하는 것과 같은 화면입니다.

Rancher UI → Cluster Management → `dev` ⋮ → Edit Config → **Registries** → Mirrors에 Hostname `192.168.64.3:5050`, Endpoint `http://192.168.64.3:5050` → Save. Rancher가 노드의 `registries.yaml`을 갱신하고 RKE2를 재시작합니다. 1~2분 뒤 확인합니다.

```bash
limactl shell dev1 -- sudo cat /etc/rancher/rke2/registries.yaml
```

UI가 번거로우면 `lab/registries.yaml`을 dev1의 `/etc/rancher/rke2/registries.yaml`로 복사하고 `sudo systemctl restart rke2-server`를 실행해도 같습니다.

## 2. 빌드와 push (Jenkins Job 역할)

```bash
./build.sh 1.0.0                 # web, was 둘 다
./build.sh 1.0.1 was             # was Job 만 돈 상황을 흉내
curl -s http://192.168.64.3:5050/v2/was-order/tags/list      # {"name":"was-order","tags":["1.0.0","1.0.1"]}
```

앱마다 하는 일은 Jenkinsfile과 같습니다. (was만) jar 빌드, 이미지 빌드(버전을 `APP_VERSION`으로 주입), push, 차트 패키지(`dist/<앱>-<버전>.tgz`). Apple Silicon에서 빌드한 이미지는 arm64이고 dev1도 arm64라 그대로 돕니다. 실서버(x86)는 Jenkins가 그쪽에서 빌드합니다.

## 3. 방식 A: 매니페스트 적용 후 Rancher에서 태그 변경

첫 배포만 kubectl로 하고, 이후 릴리즈는 Rancher UI에서 합니다.

```bash
kubectl apply -f k8s/web-was.yaml
kubectl -n webwas get pods -w                                 # web 2, was 2 Running
curl -s http://app.dev.192-168-64-3.sslip.io/api/hello        # version 1.0.0
```

Safari에서 `http://app.dev.192-168-64-3.sslip.io`를 열면 web 버전과 WAS 응답(버전, Pod 이름)이 보입니다.

**새 릴리즈.** `./build.sh 1.0.1 was`로 1.0.1을 push합니다. Rancher → dev → Workloads → Deployments → `was-order` ⋮ → Edit Config → Image를 `192.168.64.3:5050/was-order:1.0.1`로 → Save. Pod가 하나씩 교체되고 `/api/hello`의 version이 1.0.1로 바뀝니다. web-portal도 같은 방법입니다.

**롤백.** `was-order` ⋮ → Rollback → 이전 리비전 선택. 이미지만 되돌아갑니다.

**실패 확인.** 존재하지 않는 태그 `1.0.9`로 바꿔 보면 새 Pod가 `ImagePullBackOff`에 머물고 기존 Pod는 계속 서비스합니다. 롤링 업데이트가 잘못된 릴리즈를 막는 모습입니다.

## 4. 방식 B: Helm 차트를 릴리즈 단위로 (앱마다 하나)

### 차트 저장소 올리기 (mgmt)

mgmt 클러스터 context로 전환해 적용합니다. mgmt kubeconfig는 Rancher → `local` 클러스터에서 받습니다.

```bash
kubectl --context local apply -f lab/chartmuseum.yaml
curl -s http://192.168.64.2:8085/health                        # {"healthy":true}
```

Rancher UI → `dev` 클러스터 → Apps → **Repositories** → Create. Name `lab-charts`, Target `http(s) URL`, Index URL `http://192.168.64.2:8085` → Create. 상태가 Active면 Rancher Pod가 index를 읽은 것입니다.

### 차트 업로드

```bash
CHART_REPO=http://192.168.64.2:8085 ./build.sh 1.0.2
curl -s http://192.168.64.2:8085/api/charts | head -c 300     # web-portal, was-order 각 1.0.2
```

Rancher → Apps → Repositories → `lab-charts` ⋮ → Refresh. Charts 탭에 `web-portal`, `was-order` 두 개가 보입니다.

### 설치, 업그레이드, 롤백

방식 A로 올린 리소스와 이름이 겹치므로 먼저 지웁니다.

```bash
kubectl delete -f k8s/web-was.yaml
```

1. **was 먼저.** Apps → Charts → `was-order` → Install. Namespace `webwas`(새로 만들기), Name `was-order` → values 기본값 → Install.
2. **web 다음.** Apps → Charts → `web-portal` → Install. 같은 Namespace `webwas`, Name `web-portal` → Install. nginx가 같은 네임스페이스의 `was-order` Service로 프록시하므로 둘이 같은 네임스페이스에 있어야 합니다.
3. `./build.sh 1.0.3 was` 후 Repositories Refresh → Installed Apps → `was-order` → **Upgrade** → 버전 드롭다운에서 1.0.3 → 이전 values가 미리 채워져 있음 → Upgrade. WAS만 바뀌고 web Pod는 그대로입니다.
4. `was-order` ⋮ → **Rollback** → 리비전 1 선택. 이미지와 values가 그 시점으로 돌아갑니다.

values를 바꾸는 것도 릴리즈입니다. Upgrade 화면에서 `replicas`를 3으로 바꾸고 같은 차트 버전으로 Upgrade하면 리비전이 하나 올라가고 Rollback 대상이 됩니다.

## 5. 방식 A와 B에서 Rancher가 보여주는 것

| | A: Deployment 태그 변경 | B: Apps |
| --- | --- | --- |
| 릴리즈 단위 | Deployment 하나의 이미지 | 앱 하나의 차트 버전 (이미지 + 설정) |
| 배포 가능한 버전 목록 | 없음. 레지스트리에서 태그를 알아 와야 함 | 드롭다운에 보임 |
| 롤백 범위 | 이미지 | 이미지 + values |
| 서비스가 10개면 | 10번 편집 | 앱 10개, 각각 독립 업그레이드 |
| 준비 | 없음 | 앱마다 차트, 저장소 등록 1회 |

A로 흐름을 익히고 B로 가는 순서를 권합니다.

## 6. 실서버로 옮길 때 바꾸는 것

| 실습 | 실서버 |
| --- | --- |
| `build.sh` | `web/Jenkinsfile`, `was/Jenkinsfile` (각 저장소 루트로 옮기고 `dir('web')`·`dir('was')` 제거) |
| `192.168.64.3:5050` registry Pod | Nexus docker hosted `nexus:5000`. Rancher Registries에 주소·계정 |
| `192.168.64.2:8085` ChartMuseum | Nexus helm hosted `http://nexus:8081/repository/helm-hosted/`. 업로드는 `curl -u user:pw --upload-file` |
| Rancher Apps 저장소 URL | 같은 Nexus 주소. **Rancher 관리 서버 → Nexus 8081** 방화벽 필요 |
| `app.dev.192-168-64-3.sslip.io` | 사내 DNS → dev MetalLB VIP |
| arm64 이미지 | Jenkins(x86)가 빌드 |
| `tools { jdk 'temurin-21' }` | Jenkins Global Tool Configuration에 등록한 JDK 이름 |

### 6-1. Job 두 개로 끝나는 이유

```text
web 저장소 push ──▶ web Job ──▶ Nexus  web-portal:1.0.41 이미지 + web-portal-1.0.41.tgz 차트
was 저장소 push ──▶ was Job ──▶ Nexus  was-order:1.0.58  이미지 + was-order-1.0.58.tgz  차트
                                            │
담당자: Rancher → Apps → was-order → 1.0.58 선택 → Upgrade   (web 은 건드리지 않음)
```

- Job은 코드가 바뀔 때마다 자동으로 돌고, 이미지와 차트를 **같은 버전으로** 쌓습니다. 배포하지 않습니다.
- 앱마다 차트가 따로이므로 "이번 조합"을 정하는 별도 단계가 없습니다. Rancher에서 앱별로 버전을 고릅니다.
- web과 WAS의 API가 함께 바뀌는 릴리즈는 was → web 순서로 두 번 업그레이드합니다. 조합을 한 버전으로 고정해야 할 만큼 서비스가 늘면 두 차트를 의존성으로 묶는 상위 차트를 추가하면 되고, 지금 구조는 그대로 둡니다.

## 7. 막히는 지점

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| `docker push` → `http: server gave HTTP response to HTTPS client` | Docker Desktop에 insecure-registries 미설정 | 0장 JSON 추가 후 Apply |
| Pod `ImagePullBackOff`, 이벤트에 같은 HTTPS 메시지 | dev1 containerd에 mirror 미설정 | 1장 Registries 설정. `registries.yaml`이 노드에 있는지 확인 |
| Pod `ImagePullBackOff`, `manifest unknown` | 태그 오타 또는 push 전 | `curl .../v2/<이미지>/tags/list`로 태그 확인 |
| web 화면에 `WAS 연결 실패` | `was-order` Service가 다른 네임스페이스 또는 미설치 | 두 앱을 같은 네임스페이스에. was 먼저 설치 |
| 레지스트리 Pod가 `Pending` | hostPort 5050 점유 | `limactl shell dev1 -- sudo ss -lntp \| grep 5050` |
| Rancher Repositories가 `Active`가 안 됨 | mgmt에서 8085 접근 불가 또는 ChartMuseum 미기동 | `limactl shell mgmt -- curl -s http://192.168.64.2:8085/index.yaml` |
| Upgrade 드롭다운에 새 버전이 없음 | index 캐시 | Repositories → Refresh |
| `./build.sh` 중 Gradle이 느림·실패 | 첫 실행의 의존성 다운로드, 또는 사내 프록시 | 재시도. 사내망이면 `~/.gradle/gradle.properties`에 프록시 |
| WAS Pod가 `OOMKilled` | limits 512Mi에 힙이 초과 | `JAVA_TOOL_OPTIONS`의 `MaxRAMPercentage` 확인. 기본 75% |

## 8. 정리

```bash
kubectl delete ns webwas tools                       # dev1: 앱 + 레지스트리
kubectl --context local delete ns tools              # mgmt: ChartMuseum
```

Rancher → Apps → Repositories → `lab-charts` 삭제. 노드 디스크의 `/var/lib/registry-data`, `/var/lib/chartmuseum`은 남으므로 완전히 지우려면 VM에서 삭제합니다.

## 관련 문서

- CI/CD와 레지스트리의 자리, Jenkins 권한 범위: [16 Rancher로 여러 클러스터 중앙 관리](../../concepts/16-rancher-multi-cluster-management.md)
- 실서버 방화벽 리스트와 registries.yaml: [17 실서버 Rancher + RKE2 구축 준비](../../concepts/17-onprem-rancher-rke2-preparation.md)
- web/WAS를 Deployment·Service·Ingress로 나누는 기준: [10 web/WAS 워크로드 설계](../../concepts/10-web-was-workload-design.md)
- 도메인별 Ingress 분기 예제: [Ingress 라우팅](../ingress-routing/README.md)

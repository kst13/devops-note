# Rancher로 여러 클러스터 중앙 관리

Rancher는 여러 Kubernetes 클러스터를 한 화면에서 관리하는 도구입니다. 사용자와 권한을 한곳에서 통제하고, 클러스터 생성과 업그레이드를 UI에서 처리하고, 워크로드와 노드를 조회·조작합니다. 이 문서는 온프레미스에서 **Rancher + RKE2**로 dev/stage/prod를 운영하려 할 때 설계 단계에서 결정해야 하는 것들을 정리합니다. 설치 명령은 [Lima + Rancher + RKE2 실습 매뉴얼](../commands/lima-rancher-rke2-lab.md)에 있습니다.

## 1. Rancher는 Kubernetes 위에서 도는 애플리케이션이다

Rancher는 Helm 차트로 설치하는 일반 Kubernetes 애플리케이션입니다. Rancher가 설치된 클러스터를 **local 클러스터** 또는 **관리 클러스터**라고 부릅니다. Rancher가 관리하는 나머지 클러스터는 **downstream 클러스터**입니다.

Rancher의 상태 데이터(클러스터 목록, 사용자, 권한)는 별도 DB 없이 관리 클러스터의 etcd에 CRD로 저장됩니다. 그래서 Rancher 이중화는 곧 관리 클러스터 이중화입니다.

```text
[관리 클러스터]                     [downstream 클러스터]
 RKE2 또는 k3s, server 3대          dev   (RKE2)
 Rancher Pod x3                     stage (RKE2)
 cert-manager                       prod  (RKE2)
```

이 구조에서 자주 헷갈리는 점은 "Rancher를 먼저 설치한다"는 말의 뜻입니다. Rancher를 dev/stage/prod보다 먼저 설치한다는 뜻이고, 관리 클러스터의 RKE2는 Rancher보다 먼저 손으로 설치해야 합니다. Rancher가 아직 없으니 그 클러스터만은 자동화할 수 없습니다.

## 2. 관리 클러스터는 따로 둔다

Rancher를 워크로드 클러스터 안에 같이 넣지 않고 전용 클러스터에 둡니다. 공식 권장이기도 합니다.

| 분리하는 이유 | 내용 |
| --- | --- |
| 장애 범위 | 워크로드 클러스터 업그레이드나 장애가 Rancher를 같이 죽이지 않는다. 반대도 같다 |
| 권한 | Rancher는 모든 클러스터의 admin 권한을 가진다. 서비스 Pod와 같은 노드에 두면 컨테이너 탈출 한 번에 전체 인프라가 노출된다 |
| 업그레이드 순서 | Rancher 버전마다 지원하는 Kubernetes 버전 범위가 있다. 분리하면 각자 일정으로 움직일 수 있다 |
| 리소스 | 관리 대상이 늘면 Rancher 메모리 사용량도 늘어 서비스와 경합한다 |

관리 클러스터에는 Rancher 외에 다른 워크로드를 넣지 않습니다. 사양은 2 vCPU, 8GB 노드 3대면 시작할 수 있습니다. 랩이나 학습이면 1노드로 시작해도 되고, 나중에 노드를 추가해 3대로 늘릴 수 있습니다.

### 이중화 구성

| 계층 | 구성 |
| --- | --- |
| control plane / etcd | server 노드 3대. etcd quorum 때문에 홀수 |
| Rancher Pod | Helm `replicas=3`. 노드마다 하나씩 분산 |
| 진입점 | 3노드 앞에 **외부 L4 LB**(HAProxy, 장비 LB)를 두고 443 분산. Rancher hostname의 DNS가 이 LB를 가리킨다 |
| 인증서 | cert-manager 자체 서명, 사내 CA, 또는 공인 인증서 |

L4 LB는 Rancher가 제공하지 않으므로 직접 준비합니다. 처음에는 LB를 TCP 패스스루로 두고 TLS를 Rancher가 처리하게 하는 것이 단순합니다. LB에서 TLS를 종료하면 `tls=external` 설정과 websocket 통과 설정이 추가로 필요합니다.

**Rancher가 죽어도 downstream 클러스터는 계속 돌아갑니다.** 워크로드에는 영향이 없고, Rancher UI, Rancher 경유 kubeconfig, Rancher 계정 인증만 멈춥니다. 각 클러스터의 직접 kubeconfig가 있으면 kubectl은 그대로 됩니다.

## 3. Rancher hostname은 처음에 정하고 오래 쓴다

Helm 설치 시 `hostname` 값이 필수입니다. 이 값이 Ingress host 규칙과 TLS 인증서에 들어가고, Rancher의 `server-url` 설정이 됩니다. `server-url`은 모든 downstream 노드의 에이전트가 접속하는 주소로 굳어지므로, 바꾸려면 모든 노드를 재등록해야 합니다.

이 이름은 세 곳에서 풀려야 합니다.

| 위치 | 이유 |
| --- | --- |
| 운영자 PC | 브라우저로 UI 접속 |
| downstream 노드의 OS | `rancher-system-agent`가 접속 |
| downstream 클러스터 **안의 Pod** | `cattle-cluster-agent` Pod가 접속. `/etc/hosts`는 Pod에 전달되지 않으므로 놓치기 쉽다 |

사내 DNS에 레코드 하나를 추가하는 것이 가장 단순합니다. 사내 DNS가 없다면 Rancher 구축 전에 DNS 서버(dnsmasq 정도)를 두는 것을 권합니다. 노드 이름 해석과 사설 레지스트리 주소에도 계속 필요해집니다.

## 4. downstream 클러스터 등록: Custom과 Import

온프레미스에서 VM은 직접 만들더라도, 클러스터 등록 방식은 두 가지가 있습니다.

| | Custom | Import |
| --- | --- | --- |
| RKE2 설치 주체 | Rancher가 준 등록 명령을 VM에서 실행하면 **에이전트가** RKE2를 설치 | 운영자가 직접 설치한 뒤 등록만 |
| 업그레이드 | UI에서 롤링 업그레이드. etcd → control plane → worker 순서를 Rancher가 지킴 | 각 노드에서 직접 |
| etcd 스냅샷/복원 | UI에서 관리 | 직접 |
| 노드 추가 | 등록 명령 한 줄 | 직접 설치 후 join |
| 설정 저장 | RKE2 버전, CNI, 역할, taint가 Rancher 안에 하나의 객체로 저장. dev 설정을 stage, prod에 복제하기 쉬움 | 각 노드의 `config.yaml`을 일일이 확인 |
| 클러스터 소유 | Rancher | 외부. Rancher는 관찰자 |

새로 만드는 클러스터이고 Rancher를 두기로 했다면 **Custom**을 씁니다. Import는 이미 운영 중인 클러스터, 다른 도구(Terraform, Ansible)가 생명주기를 관리하는 클러스터, RKE2가 아닌 배포판을 붙일 때 씁니다.

Custom의 대가는 Rancher가 클러스터 생명주기의 단일 관리 지점이 된다는 것입니다. Rancher가 죽은 동안에는 업그레이드나 노드 추가를 할 수 없습니다. 그래서 관리 클러스터 이중화와 `rancher-backup`이 Custom 방식의 전제 조건입니다.

### Docker 컨테이너에는 Custom 등록이 안 된다

RKE2는 systemd가 있는 Linux 호스트를 전제로 합니다. 등록 명령이 설치하는 에이전트도 systemd 서비스로 RKE2를 올립니다. k3d처럼 Docker 컨테이너로 띄운 k3s는 systemd가 없고 k3s가 이미 떠 있으므로 Import만 됩니다. 로컬에서 Custom 흐름을 보려면 Lima, Multipass 같은 VM이 필요합니다. k3s든 RKE2든 VM이면 Custom이 됩니다.

## 5. 등록 명령은 에이전트를 설치하고, 에이전트가 RKE2를 설치한다

Rancher는 노드에 SSH로 들어가지 않습니다. 방향이 반대입니다. 노드에 심어진 에이전트가 Rancher로 **outbound** 접속해 할 일을 받아 갑니다.

```text
1. UI에서 Custom 클러스터 정의 (버전, CNI, 역할)      ← 설계도 준비
2. Rancher가 등록 명령(토큰 포함)을 보여줌
3. 운영자가 각 VM에서 그 명령을 실행                   ← 유일한 수동 단계
4. 명령이 rancher-system-agent를 설치하고 systemd로 시작
   └─ 여기서 명령은 끝난다. RKE2는 아직 없다.
5. 에이전트가 Rancher(443)로 접속해 "나는 dev 클러스터 노드" 라고 신고
6. Rancher가 plan("RKE2 v1.xx를 이 역할로 설치, config는 이 내용") 을 내려줌
7. 에이전트가 plan대로 RKE2 다운로드·설치·기동
8. 이후 업그레이드·설정 변경도 plan 갱신으로 처리
```

등록 명령의 각 인자는 다음을 뜻합니다.

| 인자 | 의미 |
| --- | --- |
| `--server` | 에이전트가 접속할 Rancher 주소. `server-url` 값 |
| `--token` | 이 노드가 어느 클러스터 소속인지 증명하는 등록 토큰 |
| `--ca-checksum` | Rancher 인증서 CA의 해시. 자체 서명 인증서일 때 "Insecure" 체크로 붙는다 |
| `--etcd --controlplane --worker` | 노드 역할. Rancher가 이 역할에 맞는 RKE2 설정을 내려준다 |

이 구조 때문에 방화벽은 **노드 → Rancher 443** 방향만 열면 됩니다. Rancher가 노드로 들어오는 경로는 필요 없습니다.

에이전트가 설치한 RKE2는 설정을 `/etc/rancher/rke2/config.yaml`이 아니라 `/etc/rancher/rke2/config.yaml.d/50-rancher.yaml`에 둡니다. Rancher가 관리하는 설정과 운영자가 추가하는 설정을 분리하기 위해서입니다. 운영자가 직접 설치한 관리 클러스터와 파일 위치가 다른 것은 설치 주체가 다르기 때문입니다.

## 6. Rancher가 제어할 수 있는 범위

### 노드

| 작업 | Node Driver(vSphere 등 API 연동) | Custom | Import |
| --- | --- | --- | --- |
| cordon / drain / uncordon | O | O | O |
| 노드 수 증감 | O (VM 생성·삭제까지) | 등록 명령 수동 실행 | X |
| 특정 노드 교체 | O | 수동 | X |
| VM 전원 on/off | X | X | X |

Custom 방식에서 Rancher가 하는 노드 제어는 Kubernetes 노드 객체 수준입니다. VM 전원은 하이퍼바이저에서 해야 합니다. 유지보수 순서는 "Rancher에서 drain → 하이퍼바이저에서 VM 작업 → Rancher에서 uncordon"입니다.

### 워크로드

클러스터 종류와 무관하게 전부 됩니다. Rancher가 downstream 클러스터의 Kubernetes API를 대신 호출하므로 kubectl로 되는 일은 UI에서도 됩니다.

| 작업 | UI | kubectl |
| --- | --- | --- |
| 내리기 / 올리기 | Deployment → Scale 0 / N | `kubectl scale deploy/was-order --replicas=0` |
| 재시작 | Redeploy | `kubectl rollout restart deploy/was-order` |
| 이미지 교체 | Edit Config | `kubectl set image ...` |
| 롤백 | Rollback → 리비전 선택 | `kubectl rollout undo` |
| 로그 / 셸 | View Logs / Execute Shell | `kubectl logs`, `kubectl exec` |

WAS마다 Deployment를 따로 만들면 각각 독립적으로 내리고 올립니다. 같은 Deployment의 Pod 중 "특정 인스턴스 하나"만 내리는 것은 안 됩니다. Deployment의 Pod는 구별되지 않는 복제본이라 하나를 지우면 바로 새 Pod가 뜹니다. 인스턴스를 구별해야 하면 인스턴스별 Deployment 또는 StatefulSet을 씁니다. 그 전에 구별이 정말 필요한지 따져보는 것이 먼저입니다.

### Project

Rancher 전용 계층으로, 네임스페이스 여러 개를 묶어 권한, 리소스 쿼터, 네트워크 정책을 한 단위로 관리합니다. 팀별로 Project를 나누면 각 팀은 자기 Project의 리소스만 UI에서 보고 조작합니다. 권한은 네임스페이스 단위 RBAC으로 내려갑니다.

```text
dev 클러스터
 ├─ Project: order-team   → Namespace order   → web-order, was-order
 ├─ Project: member-team  → Namespace member  → web-member, was-member
 └─ Project: shared       → Namespace infra   → redis, kafka
```

## 7. dev/stage/prod 배치

| 클러스터 | 내용 |
| --- | --- |
| 관리 | Rancher만 |
| non-prod | dev, stage를 Project로 분리. 리소스 쿼터와 NetworkPolicy로 간섭 차단 |
| prod | prod만. 업그레이드는 non-prod에서 검증 후 |

prod를 분리하는 핵심 이유는 **클러스터 업그레이드가 곧 prod 업그레이드가 되기 때문**입니다. 네임스페이스는 노드, CNI, CRD, StorageClass, Ingress Controller를 공유하므로 격리 경계가 약합니다. 자세한 토폴로지 논의는 [07 실서버 클러스터 토폴로지](07-production-cluster-topology.md)에 있습니다.

노드 역할 표기에서 "Server/Worker 3대"는 3대 모두 `rke2-server`이고 taint를 걸지 않아 워크로드도 함께 뜨는 겸임 구성입니다. prod처럼 control plane과 worker를 분리할 때는 control plane에 `CriticalAddonsOnly=true:NoExecute` taint를 걸어 서비스 Pod가 뜨지 않게 합니다. taint는 노드가 Pod를 거부하는 표시이고, toleration을 가진 Pod만 그 노드에 뜹니다. RKE2와 k3s는 server 노드에 기본으로 taint를 걸지 않습니다.

## 8. 외부/내부 트래픽 분리

같은 URL로 외부망과 내부망 접근을 다르게 처리하는 요구는 **L4가 아니라 클러스터 안의 Ingress에서** 해결합니다. L4는 트래픽을 노드까지만 던져주고, 어떻게 나눌지는 Ingress 리소스가 결정합니다. 라우팅 규칙이 인프라팀 장비에서 운영팀이 관리하는 Kubernetes 리소스로 옮겨 오므로, 변경할 때마다 인프라팀에 요청할 일이 줄어듭니다.

| 패턴 | 구분 기준 | 내용 |
| --- | --- | --- |
| A. 진입점 분리 (권장) | **어느 문으로 들어왔나** | Ingress Controller를 `external`, `internal` 두 벌로 띄우고, 사내 DNS와 외부 DNS가 같은 이름을 다른 IP로 응답(split-horizon). 외부용 Ingress에는 공개 경로만, 내부용에는 전체 경로 |
| B. 출발지 IP 판별 | **어디서 왔나** | 진입점 하나에서 클라이언트 IP로 분기. NGINX는 `whitelist-source-range`로 차단만, Traefik은 `ClientIP()` 매처로 백엔드 분기 가능 |

패턴 B는 Ingress Controller가 **진짜 클라이언트 IP**를 봐야 합니다. L4가 SNAT을 하면 전부 LB IP로 보여 구분이 불가능합니다. L4에서 출발지 IP 보존(DSR) 또는 Proxy Protocol을 켜고, Ingress Service에 `externalTrafficPolicy: Local`을 둡니다. `X-Forwarded-For`는 위조 가능하므로 신뢰하는 프록시 IP에서 온 것만 인정하도록 설정해야 합니다.

어느 패턴이든 인프라팀에 요청하는 것은 "443을 클러스터 노드(또는 VIP)로 보내달라"와, B일 때 "출발지 IP를 보존해달라" 두 가지뿐입니다. 이후 도메인 추가와 경로 변경은 Ingress 리소스 수정입니다. Ingress의 위치와 동작은 [09 외부 트래픽 진입점](09-external-traffic-entrypoint.md)에 있습니다.

## 9. CI/CD와 레지스트리의 자리

Jenkins는 **클러스터 바깥에서 kubectl을 쓰는 또 하나의 운영자**입니다. 노드에 SSH로 들어가지 않고, kubeconfig로 Kubernetes API에 "이 이미지로 바꿔라"고 요청합니다.

| | A. Push (Jenkins가 직접 배포) | B. GitOps (Fleet) |
| --- | --- | --- |
| 흐름 | 빌드 → 이미지 push → `kubectl set image` | 빌드 → 이미지 push → 매니페스트 저장소의 태그만 commit → Fleet이 반영 |
| Jenkins 권한 | 클러스터 쓰기 권한 | Git 쓰기만 |
| 롤백 | 이전 빌드 재실행 | `git revert` |
| 적합 | dev | stage, prod |

Rancher가 관여하는 지점은 세 가지입니다. Jenkins 전용 사용자를 만들어 특정 Project의 Member로만 넣어 kubeconfig 범위를 제한하는 것, Fleet(Rancher 내장 GitOps)으로 Git 저장소를 클러스터에 매핑하는 것, 배포 결과를 Workloads 화면에서 확인하고 롤백하는 것입니다.

이미지 레지스트리는 반드시 하나 필요합니다. 노드가 이미지를 **pull**하는 구조이기 때문입니다. Nexus가 이미 있으면 docker (hosted) 저장소를 추가하는 것으로 충분하고 Harbor를 새로 둘 필요가 없습니다. Docker Hub proxy 저장소도 함께 두면 익명 pull 횟수 제한(IP당 시간당 10회)에 걸리지 않습니다. 노드가 사설 레지스트리에 인증하는 설정은 Rancher UI의 Cluster Configuration → Registries에서 넣으면 모든 노드의 `registries.yaml`에 내려갑니다.

Jenkins, Nexus, Harbor는 관리 클러스터에 넣지 않습니다. 별도 VM이나 non-prod 클러스터에 둡니다.

## 10. 설계 체크리스트

- [ ] 관리 클러스터를 전용으로 분리했는가. 3노드인가
- [ ] Rancher hostname을 사내 DNS에 등록했는가. Pod에서도 풀리는가
- [ ] 관리 클러스터 앞 L4 LB와 prod control plane 앞 VIP를 준비했는가
- [ ] downstream은 Custom으로 등록하는가. Import를 고른 이유가 있는가
- [ ] 노드 → Rancher 443 outbound 방화벽이 열려 있는가
- [ ] prod control plane에 taint를 걸었는가. dev/stage 겸임 구성을 문서에 명시했는가
- [ ] `rancher-backup`과 downstream etcd 스냅샷 주기를 설정했는가
- [ ] 외부/내부 분리 패턴을 정했는가. B라면 출발지 IP 보존을 확인했는가
- [ ] 이미지 레지스트리와 Docker Hub proxy가 있는가
- [ ] Jenkins 계정의 kubeconfig 범위가 Project로 제한되는가

## 관련 문서

- 로컬에서 같은 구조를 재현하는 절차: [Lima + Rancher + RKE2 실습 매뉴얼](../commands/lima-rancher-rke2-lab.md)
- RKE2 설정 키와 부품: [14 RKE2](14-rke2-distribution.md)
- 관리 클러스터를 3대로 올리는 절차: [15 RKE2 3대 클러스터 구축](15-rke2-three-node-build.md)
- 클러스터를 몇 개로 나누는가: [07 실서버 클러스터 토폴로지](07-production-cluster-topology.md)
- Ingress와 L4의 역할 분담: [09 외부 트래픽 진입점](09-external-traffic-entrypoint.md)
- 네임스페이스 권한과 Pod 보안: [12 접근 제어와 Pod 보안](12-access-control-and-pod-security.md)

## 참고한 공식 문서

- Rancher Manager Documentation — Installation Requirements, Install/Upgrade on a Kubernetes Cluster, Custom Clusters, Registering Existing Clusters, Backups and Disaster Recovery (2026-10-02 확인, Rancher v2.15 기준)
- Rancher System Agent — `rancher/system-agent` README
- RKE2 Documentation — Configuration File (`config.yaml.d`), Private Registry Configuration

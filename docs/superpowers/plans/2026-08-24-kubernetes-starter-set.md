# 쿠버네티스 스타터 세트 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `kubernetes/` 토픽(README + concepts 5개 + kubectl 치트시트)을 신설하고 web에 등록한다.

**Architecture:** 로컬 실습은 k3d(k3s in Docker), 실서버는 k3s를 기준으로 한다. 02 문서가 전체 지형(로컬 도구·실서버 배포판·관리형·환경 전략)을 다루고, 03·04는 k3d 실습, 05는 실서버 k3s 설치를 다룬다. 스펙: `docs/superpowers/specs/2026-08-24-kubernetes-starter-set-design.md`.

**Tech Stack:** Markdown(자체 렌더러 서브셋), k3d/k3s/kubectl, Next.js 콘텐츠 파이프라인(`web/scripts/sync-content.mjs`).

## Global Constraints

- 모든 산문은 한국어. ATX 헤딩, 언어 태그 붙은 펜스 코드 블록, 상대 링크. 파일명은 lowercase kebab-case.
- **렌더러 서브셋 준수**: `##`/`###` 헤딩만(문서 제목 `#`은 1개), 리스트 중첩 금지, 지원 인라인은 `` `code` ``·`**bold**`·`[text](link)`·bare URL뿐. 각주·이미지·HTML 태그 사용 금지.
- Summary 추출 규칙: 문서 상단 H1 바로 아래에 "실질적인 일반 문단"(리스트·표·헤딩 아님)을 반드시 둔다 — 이 문단이 카드 요약이 된다.
- 난이도 배지는 파일명 접두사로 자동 결정: 01~04 → 입문, 05 → 중급, commands/ → 참고. 접두사를 바꾸지 않는다.
- 시크릿·호스트는 플레이스홀더(`${K3S_TOKEN}`, `server-1.example.internal` 등). 실제 값 커밋 금지.
- 설정·명령은 "왜 필요한가"를 함께 설명. YAML 들여쓰기 2칸.
- `web/app/data/content.generated.json`은 생성 파일 — 손으로 편집 금지.
- 커밋: CLAUDE.md 규약(토픽당 1커밋)에 따라 모든 파일을 검증 후 **하나의 커밋**으로 만든다. 제목 예: `Add Kubernetes starter guide series`.
- 버전 표기: k3s는 v1.31+ 기준, k3d는 v5 기준으로 작성하되 "사용 중인 버전 확인" 문구를 README 학습 기준에 둔다.

## 문서 간 인터페이스 (모든 태스크 공통)

- 상호 링크는 실제 상대 경로로: 예) `concepts/02-ways-to-run-kubernetes.md`에서 `[k3s 설치](05-k3s-cluster-installation.md)`, README에서 `[쿠버네티스 기본기](concepts/01-kubernetes-basics.md)`, 01에서 docker 토픽으로 `[컨테이너와 이미지](../../docker/concepts/01-container-image.md)`.
- 실습 클러스터 이름은 전 문서 공통 `devops-note`: `k3d cluster create devops-note`.
- 실습 네임스페이스는 기본 `default`를 사용(입문 문서이므로 네임스페이스 개념은 01에서 소개만).
- 예제 앱 이미지는 전 문서 공통 `nginx:1.27-alpine`.

---

### Task 1: kubernetes/README.md

**Files:**
- Create: `kubernetes/README.md`

- [ ] **Step 1: README 작성**

구성(redis/README.md와 같은 골격):

- H1 `# Kubernetes` + 요약 문단(토픽이 다루는 범위: 개념, 실행 방법 비교, k3d 실습, 실서버 k3s 설치)
- `## 문서 구조` — text 코드 블록으로 `concepts/`, `commands/` 설명
- `## 추천 학습 순서` — `### 입문`(01→02→03→04 + kubectl 치트시트), `### 실서버`(05) 두 단계, 각 항목은 순서 리스트 + 상대 링크
- `## 학습 기준` — 리스트 5개 내외:
  - 예제는 k3d(k3s in Docker)로 검증하며, 실서버 절차는 k3s 기준이다
  - YAML 매니페스트는 어느 배포판·관리형에서도 동일하게 동작하는 것을 우선한다(`ingressClassName` 등 명시)
  - 도구 버전(k3d v5, k3s v1.31+)과 사용 중인 버전의 차이를 확인한다
  - 컨테이너 기초는 docker 토픽을 먼저 본다 (상대 링크)
- `## 참고 기준` — 2026-08-24 기준 쿠버네티스·k3s 공식 문서 우선 참고 문구

- [ ] **Step 2: 렌더러 서브셋 점검** — 중첩 리스트·미지원 문법 없는지 훑기

### Task 2: concepts/01-kubernetes-basics.md

**Files:**
- Create: `kubernetes/concepts/01-kubernetes-basics.md`

- [ ] **Step 1: 문서 작성**

섹션 구성:

- H1 `# 쿠버네티스 기본기` + 요약 문단
- `## 왜 쿠버네티스인가` — Docker Compose 한 대 운영의 한계(서버 여러 대, 장애 자동 복구, 무중단 배포, 스케일)에서 출발. docker 토픽 상대 링크.
- `## 클러스터 구조` — Control Plane(kube-apiserver, scheduler, controller-manager, etcd)과 Node(kubelet, 컨테이너 런타임, kube-proxy)를 표로 정리. "모든 조작은 API 서버를 통한다" 강조.
- `## 선언적 모델` — 명령형 vs 선언형 비교. "원하는 상태를 YAML로 선언하면 컨트롤러가 현재 상태를 수렴시킨다". 최소 YAML 예시 1개(Deployment 축약)와 `kubectl apply -f` 한 줄.
- `## 핵심 오브젝트 한눈에` — Pod/Deployment/Service/ConfigMap/Namespace 5개를 표로: 이름, 한 줄 역할, 자세히 다루는 문서 링크(03·04).
- `## k3s는 무엇이 다른가` — 단일 바이너리, sqlite 기본 datastore, Traefik·ServiceLB·local-path 내장, 레거시 기능 제거. "API는 동일하므로 여기서 배우는 내용은 모든 쿠버네티스에 통한다" 명시. 02 문서 링크.

- [ ] **Step 2: 렌더러 서브셋 점검**

### Task 3: concepts/02-ways-to-run-kubernetes.md

**Files:**
- Create: `kubernetes/concepts/02-ways-to-run-kubernetes.md`

- [ ] **Step 1: 문서 작성**

설계 논의의 핵심 문서. 섹션 구성:

- H1 `# 쿠버네티스를 경험하는 방법들` + 요약 문단("같은 쿠버네티스라도 실행하는 방법에 따라 배우는 것이 다르다")
- `## 로컬 실습 도구` — k3d / kind / minikube / Docker Desktop·Rancher Desktop 비교 표(구조, 속도, 특징, 언제 고르나). kind가 로컬·CI의 사실상 표준 도구임을 명시하고, k3d 선택 이유(실서버 k3s와 같은 스택)를 문단으로.
- `## 브라우저 실습` — Killercoda 등 무설치 플레이그라운드 소개(계정만으로 즉시 실습, 영속성 없음).
- `## 실서버 직접 구축` — 3자 비교 표 + 해설 문단:
  - kubeadm: 표준 부품·직접 조립 — 내부 구조 학습, CKA 기준
  - RKE2: 표준 부품·완성품 — 온프레미스 프로덕션, CIS 강화 기본, etcd·ingress-nginx
  - k3s: 경량 부품·완성품 — 엣지·소규모·개발, sqlite 기본
  - OpenShift는 대기업 상용 영역 한 줄 언급
- `## 관리형 서비스` — EKS/GKE/AKS. Control Plane 위임의 의미(etcd 백업·업그레이드·HA가 사라짐). AWS 결합 개요: ALB 자동 생성, IRSA/Pod Identity, EBS CSI. ECS라는 비-쿠버네티스 대안 존재도 한 문단.
- `## 환경별 구성 전략` — 로컬 k3d → 개발 k3s → 스테이지·운영은 같은 배포판(RKE2 또는 EKS). "스테이지는 운영의 축소판" 원칙과 그 이유(가장자리 차이 — Ingress·LB·스토리지 — 가 스테이지에서 걸러져야 함). 이식성 규칙: `ingressClassName` 항상 명시, 배포판 기본값에 의존하지 않기.
- `## 이 시리즈의 선택` — 로컬 k3d + 실서버 k3s인 이유, kubectl 실습은 kind에서도 동일하게 따라올 수 있음 명시. 03·05 문서 링크.

- [ ] **Step 2: 렌더러 서브셋 점검** — 표가 많은 문서이므로 파이프 표 문법 확인

### Task 4: concepts/03-pod-deployment-service.md

**Files:**
- Create: `kubernetes/concepts/03-pod-deployment-service.md`

- [ ] **Step 1: 문서 작성**

섹션 구성과 필수 코드 블록:

- H1 `# Pod, Deployment, Service` + 요약 문단
- `## 실습 준비` — k3d 설치와 클러스터 생성:

```bash
brew install k3d kubectl   # macOS. 다른 OS는 k3d 공식 설치 스크립트 사용
k3d cluster create devops-note
kubectl get nodes          # Ready 노드 1개가 보이면 성공
```

- `## Pod: 최소 실행 단위` — Pod 개념(컨테이너 1개 이상 + 공유 네트워크). 단독 Pod YAML(`nginx:1.27-alpine`) 적용 → `kubectl get pods` → `kubectl delete pod`. "Pod를 직접 만들지 않는 이유"(죽어도 되살아나지 않음)로 다음 절 연결.
- `## Deployment: 원하는 상태 유지` — replicas 2의 Deployment YAML 전체 제시:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 2
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: web
          image: nginx:1.27-alpine
          ports:
            - containerPort: 80
```

  실습 흐름: `kubectl apply -f deployment.yaml` → Pod 삭제 후 자동 복구 관찰 → `kubectl scale deployment web --replicas=4` → `kubectl set image deployment/web web=nginx:1.28-alpine`으로 롤링 업데이트 → `kubectl rollout status`/`kubectl rollout undo`.
- `## Service: 고정된 접근 지점` — Pod IP는 바뀌므로 Service가 필요하다는 동기. ClusterIP Service YAML(selector `app: web`, port 80) + `kubectl port-forward svc/web 8080:80`으로 로컬 확인. Service 타입 3종(ClusterIP/NodePort/LoadBalancer) 표 — LoadBalancer는 환경마다 구현이 다르다는 점(02 문서 링크)을 명시.
- `## 정리` — `k3d cluster delete devops-note` 또는 리소스만 삭제. 이 YAML들이 어느 배포판에서도 그대로 동작함(이식성) 재강조. 04 문서 링크.

- [ ] **Step 2: 렌더러 서브셋 점검**

### Task 5: concepts/04-config-and-volume.md

**Files:**
- Create: `kubernetes/concepts/04-config-and-volume.md`

- [ ] **Step 1: 문서 작성**

섹션 구성과 필수 코드 블록:

- H1 `# 설정과 볼륨` + 요약 문단(설정을 이미지에 굽지 않는 이유 — 같은 이미지로 환경만 바꿔 배포)
- `## ConfigMap` — 생성 두 방법(`kubectl create configmap --from-literal`, YAML). 주입 두 방법: `envFrom`으로 환경 변수, `volumeMounts`로 파일. 각각 최소 YAML 스니펫과 확인 명령(`kubectl exec ... -- env`, `kubectl exec ... -- cat`).
- `## Secret` — ConfigMap과의 차이(base64 인코딩일 뿐 암호화가 아님 — etcd 암호화·외부 시크릿 관리가 별도 주제임을 명시). `kubectl create secret generic` + 플레이스홀더 값(`${DB_PASSWORD}`) 예시. 환경 변수 주입 스니펫.
- `## 볼륨 기초` — 3단 구분을 표로: emptyDir(Pod 수명), hostPath(노드 수명, 실습 외 비권장), PVC(클러스터가 관리). PVC YAML(1Gi, `storageClassName` 생략 시 기본 StorageClass 사용 — k3d/k3s에서는 local-path) + Deployment에 마운트하는 스니펫 + `kubectl get pvc,pv` 확인.
- `## StorageClass가 환경마다 다른 이유` — local-path(k3s) vs EBS CSI(EKS) 등, "PVC라는 요청 인터페이스는 같고 구현이 다르다". 02 문서 링크.
- `## 정리` — 05(실서버 설치)로 연결.

- [ ] **Step 2: 렌더러 서브셋 점검**

### Task 6: concepts/05-k3s-cluster-installation.md

**Files:**
- Create: `kubernetes/concepts/05-k3s-cluster-installation.md`

- [ ] **Step 1: 문서 작성**

섹션 구성과 필수 코드 블록:

- H1 `# k3s 클러스터 설치` + 요약 문단(로컬 k3d에서 배운 것을 실서버로)
- `## 준비물` — 리눅스 서버 1~3대(2CPU/2GB 이상 권장), 방화벽 포트 표(6443 API, 8472/udp Flannel VXLAN, 10250 kubelet).
- `## 단일 노드 설치` —

```bash
curl -sfL https://get.k3s.io | sh -
sudo k3s kubectl get nodes
```

  설치 스크립트가 하는 일(바이너리 설치, systemd 서비스 등록, kubeconfig 생성)을 문단으로 설명. `systemctl status k3s` 확인.
- `## 멀티 노드 구성` — server 노드에서 토큰 확인(`/var/lib/rancher/k3s/server/node-token`), agent 조인:

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://server-1.example.internal:6443 \
  K3S_TOKEN=${K3S_TOKEN} sh -
```

  server/agent 역할 구분(Control Plane 유무), HA가 필요하면 server 3대 + 내장 etcd(`--cluster-init`)라는 확장 경로를 문단으로 소개(상세 절차는 범위 밖 명시).
- `## 기본 내장 컴포넌트` — Traefik(Ingress), ServiceLB, local-path-provisioner, Flannel을 표로: 역할, 표준 구성과의 차이, 비활성화 옵션(`--disable traefik` 등). "표준 구성(예: RKE2의 ingress-nginx)과 다른 지점"임을 02 문서 링크와 함께 명시.
- `## 외부에서 kubectl 접속` — `/etc/rancher/k3s/k3s.yaml`을 로컬로 복사, `server:` 주소를 서버 호스트명으로 교체, `KUBECONFIG` 환경 변수 지정. kubeconfig가 곧 관리자 자격 증명이므로 유출 주의 문단.
- `## 제거` — `k3s-uninstall.sh` / `k3s-agent-uninstall.sh` 한 절.

- [ ] **Step 2: 렌더러 서브셋 점검** — 플레이스홀더(`${K3S_TOKEN}`, `server-1.example.internal`) 확인, 실제 값 없는지 확인

### Task 7: commands/kubectl-cheatsheet.md

**Files:**
- Create: `kubernetes/commands/kubectl-cheatsheet.md`

- [ ] **Step 1: 문서 작성**

섹션 구성(redis/docker 치트시트 형식 준용 — 절마다 bash 블록 + 짧은 해설):

- H1 `# kubectl 치트시트` + 요약 문단
- `## 컨텍스트와 클러스터` — `kubectl config get-contexts / use-context / current-context`, `kubectl cluster-info`
- `## 조회` — `get`(-o wide, -o yaml, -A, --watch), `describe`, `get events --sort-by=.lastTimestamp`
- `## 로그와 디버깅` — `logs`(-f, --previous, -l 라벨), `exec -it -- sh`, `port-forward`, `debug`(에페메랄 컨테이너 한 줄 소개)
- `## 리소스 조작` — `apply -f`, `delete -f`, `scale`, `rollout status/history/undo`, `edit`은 GitOps 관점에서 지양한다는 주석
- `## 자주 쓰는 진단 순서` — Pod가 안 뜰 때: `get pods` → `describe pod`(Events) → `logs --previous` 순서를 리스트로
- `## 다음 도구: k9s` — kubectl에 익숙해진 다음 쓰는 터미널 UI라는 소개 2~3문단(`brew install k9s`, `:pod` 화면 전환·`l` 로그 정도만)

- [ ] **Step 2: 렌더러 서브셋 점검**

### Task 8: web/content.config.json 등록

**Files:**
- Modify: `web/content.config.json` (aws 항목 뒤에 추가)

- [ ] **Step 1: kubernetes 항목 추가**

```json
"kubernetes": {
  "title": "Kubernetes",
  "kicker": "CONTAINER ORCHESTRATION",
  "description": "로컬 k3d 실습부터 실서버 k3s 클러스터 설치까지, 쿠버네티스를 경험하는 다양한 방법을 정리합니다.",
  "accent": "#326ce5",
  "accentSoft": "#e7eefc",
  "order": 5,
  "tags": ["Orchestration", "k3s", "kubectl"]
}
```

aws 항목 뒤에 쉼표 추가 후 삽입. JSON 유효성은 Task 9의 sync-content가 검증한다.

### Task 9: 검증

- [ ] **Step 1: 콘텐츠 동기화 및 산출 확인**

Run: `cd web && npm run sync-content`
Expected: 종료 코드 0. `git diff --stat web/app/data/content.generated.json`에 kubernetes 문서 7건이 반영된 변경이 보인다.

- [ ] **Step 2: 린트**

Run: `cd web && npm run lint`
Expected: 에러 0

- [ ] **Step 3: 빌드 + 테스트**

Run: `cd web && npm test`
Expected: 빌드 성공, `tests/rendered-html.test.mjs` 2케이스 PASS. 실패 시 테스트가 단언하는 리터럴(토픽 id·페이지 문자열)을 읽고 원인 파악 — 테스트를 콘텐츠에 맞게 고치는 것이 아니라 무엇이 깨졌는지 먼저 확인한다.

- [ ] **Step 4: 공백 오류 확인**

Run: `git diff --check` (staged 전이므로 `git diff --check` + `git diff --cached --check`)
Expected: 출력 없음

### Task 10: 커밋

- [ ] **Step 1: 스테이징과 커밋 (토픽당 1커밋 규약)**

```bash
git add kubernetes web/content.config.json web/app/data/content.generated.json
git commit -m "Add Kubernetes starter guide series"
```

주의: `content.generated.json`은 손편집 금지지만 sync 산출물 커밋은 기존 관행을 따른다 — 커밋 전 `git log --oneline -- web/app/data/content.generated.json`으로 기존에 커밋되어 온 파일인지 확인하고, 커밋 이력이 없다면(.gitignore 대상) 스테이징에서 제외한다.

## Self-Review 결과

- 스펙 커버리지: 스펙의 문서 7건(README 포함) → Task 1~7, web 등록 → Task 8, 검증 → Task 9. 누락 없음.
- 플레이스홀더 스캔: "TBD"류 없음. 각 문서 태스크는 섹션 구성·필수 코드 블록·확인 명령을 명시.
- 일관성: 클러스터 이름 `devops-note`, 이미지 `nginx:1.27-alpine`, 링크 상대 경로 규약이 태스크 간 동일.

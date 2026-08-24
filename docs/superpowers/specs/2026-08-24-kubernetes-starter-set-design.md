# 쿠버네티스 스타터 세트 설계

- 날짜: 2026-08-24
- 상태: 승인됨
- 범위: `kubernetes/` 토픽 신설 (README + concepts 5개 + kubectl 치트시트) 및 web 등록

## 목표

DevOps 지식 베이스에 쿠버네티스 토픽을 추가한다. 목표는 두 가지다.

1. 쿠버네티스를 경험하는 다양한 방법(로컬 도구, 실서버 배포판, 관리형 서비스)의 전체 지형을 보여준다.
2. 로컬 실습에서 실서버 설치·운영까지 같은 스택으로 이어지는 학습 경로를 제공한다.

## 기준 도구 선택

- **로컬 실습: k3d** (k3s in Docker) — macOS에서 즉시 실행되고, 실서버 기준인 k3s와 같은 기반이라 Traefik·ServiceLB·local-path 스토리지까지 동일하게 경험된다.
- **실서버: k3s** — CNCF 인증 경량 배포판. 단일 바이너리 설치와 토큰 조인으로 "클러스터를 직접 설치·운영하는" 경험을 낮은 진입 장벽으로 제공한다.
- kind는 로컬 표준 도구로서 02 문서에서 정식으로 소개하되, 이 시리즈의 기준은 로컬→실서버 일관성을 위해 k3d로 한다. kubectl 실습(03·04)은 어느 도구를 써도 동일하게 동작함을 문서에 명시한다.

## 디렉터리 구조

```text
kubernetes/
├── README.md                            # 토픽 소개 + 추천 학습 순서
├── concepts/
│   ├── 01-kubernetes-basics.md          # 입문
│   ├── 02-ways-to-run-kubernetes.md     # 입문
│   ├── 03-pod-deployment-service.md     # 입문
│   ├── 04-config-and-volume.md          # 입문
│   └── 05-k3s-cluster-installation.md   # 중급 (접두사 5 → 난이도 배지 자동)
└── commands/
    └── kubectl-cheatsheet.md            # 참고
```

`troubleshooting/`, `examples/`는 이번 범위에서 제외한다. 실제 이슈·실습이 쌓일 때 추가한다.

## 문서별 내용

### 01-kubernetes-basics.md

- 왜 쿠버네티스인가 (컨테이너 여러 개를 여러 서버에서 운영할 때 생기는 문제)
- 클러스터 구조: Control Plane(API 서버, 스케줄러, etcd)과 Node(kubelet, 컨테이너 런타임)
- 선언적 모델: 원하는 상태를 YAML로 선언하면 클러스터가 맞춰가는 방식
- k3s가 표준 구성과 다른 점(단일 바이너리, sqlite 기본, Traefik 등 내장) 한 절
- docker 토픽(컨테이너 개념)과 상호 참조 링크

### 02-ways-to-run-kubernetes.md

이번 설계 논의의 핵심 재료가 들어가는 문서.

- **로컬**: k3d, kind, minikube, Docker Desktop/Rancher Desktop 비교. kind가 로컬·CI의 사실상 표준 도구라는 점과 각 도구의 선택 기준 명시.
- **브라우저**: Killercoda 등 무설치 플레이그라운드.
- **실서버 직접 구축** 3자 비교:
  - kubeadm — 표준 부품, 직접 조립 (학습·CKA 기준)
  - RKE2 — 표준 부품, 완성품 (온프레미스 프로덕션, CIS 강화 기본)
  - k3s — 경량화 부품, 완성품 (엣지·소규모·개발)
  - OpenShift는 대기업 상용 영역으로 한 줄 언급.
- **관리형**: EKS/GKE/AKS — Control Plane 위임의 의미, AWS 결합 개요(ALB 자동 생성, IRSA/Pod Identity, EBS CSI), ECS라는 대안 존재.
- **환경별 구성 전략**: 로컬 k3d → 개발 k3s → 스테이지·운영은 같은 배포판(RKE2 또는 EKS). "스테이지는 운영의 축소판" 원칙. `ingressClassName` 명시 같은 배포판 이식성 규칙.
- 이 시리즈가 k3d + k3s를 기준으로 삼는 이유 명시.

### 03-pod-deployment-service.md

- 핵심 워크로드 3종: Pod, Deployment, Service
- k3d 클러스터 생성부터 시작하는 따라하기 실습 (YAML 적용, 스케일, 롤링 업데이트, Service 노출)
- YAML은 어느 배포판·관리형에서든 동일하게 동작함(이식성)을 강조

### 04-config-and-volume.md

- ConfigMap/Secret으로 설정·비밀 분리, 환경 변수·파일 마운트 주입
- 볼륨 기초: emptyDir, PersistentVolumeClaim, StorageClass 개념
- k3d 실습 기준. 시크릿 값은 플레이스홀더 규약 준수.

### 05-k3s-cluster-installation.md

- 실서버 k3s: 단일 노드 설치 → server/agent 토큰 조인으로 멀티 노드 구성
- 내장 컴포넌트(Traefik, ServiceLB, local-path-provisioner) 설명과 표준 구성과의 차이
- 외부에서 kubeconfig로 접속하는 방법
- 호스트명·토큰은 플레이스홀더(`${K3S_TOKEN}` 등) 처리. redis의 `05-redis-cluster-installation.md`와 같은 위상.

### commands/kubectl-cheatsheet.md

- 조회(get/describe/logs/events), 디버깅(exec/port-forward), 리소스 조작(apply/delete/rollout), 컨텍스트 관리
- 말미에 k9s를 "kubectl에 익숙해진 다음 쓰는 도구"로 짧게 소개

## web 등록

`web/content.config.json`에 추가:

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

accent는 쿠버네티스 브랜드 블루(#326ce5), docker의 #2f7cf6과 구분되는 톤.

## 작성 규약

- 전부 한국어, ATX 헤딩, 언어 태그 있는 펜스 코드 블록, kebab-case 파일명
- 웹 렌더러의 Markdown 서브셋 준수 (중첩 리스트 금지, 지원 문법만 사용)
- "왜 이 설정이 필요한가" 중심 설명
- docker 토픽과 상호 참조는 실제 상대 경로로 (in-app 내비게이션 변환 대상)

## 검증

- `git diff --check`
- `cd web && npm run lint && npm test` — 테스트가 리터럴 문자열·토픽 id를 단언하므로 새 토픽 추가 후 통과 확인 필수
- 새 문서가 사이트에서 렌더링되는지 확인 (`npm run dev`)

## 이후 확장 후보 (이번 범위 아님)

- RKE2 설치 문서, Rancher 관리 플랫폼, GitOps(ArgoCD), 프로덕션 아키텍처(`06-production-architecture.md`)
- troubleshooting/ 및 examples/ 디렉터리

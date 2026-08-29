# 접근 제어와 Pod 보안

세 가지 질문에 답하는 문서입니다. 누가 클러스터 API에 무엇을 할 수 있는가(RBAC), Pod가 API를 호출할 때는 누구로 취급되는가(ServiceAccount), 컨테이너는 노드 위에서 어떤 권한으로 도는가(SecurityContext). CKAD 최대 비중 도메인(환경·구성·보안 25%)의 핵심이면서, 운영 클러스터를 여러 사람·여러 앱이 쓰기 시작하면 바로 필요해지는 내용입니다.

## 요청이 통과하는 세 관문

kubectl이든 Pod 안의 앱이든, API 서버에 도착한 모든 요청은 세 단계를 거칩니다.

```text
요청 → 인증(누구인가?) → 인가(그 사람이 이걸 해도 되는가?) → 어드미션(정책 검사) → 실행
```

인증은 kubeconfig의 인증서나 ServiceAccount 토큰이 담당하고, 인가가 이 문서의 주제인 RBAC입니다. 어드미션은 통과한 요청을 정책으로 한 번 더 거르는 단계(리소스 한도 강제 등)로, 존재만 알아두면 됩니다.

## RBAC: 역할을 정의하고 묶는다

RBAC는 리소스 두 쌍으로 구성됩니다. "무엇을 할 수 있는가"를 정의하는 Role과, "누구에게 그 역할을 주는가"를 정의하는 RoleBinding입니다.

| 리소스 | 범위 | 용도 |
| --- | --- | --- |
| Role / RoleBinding | 네임스페이스 하나 | 특정 네임스페이스 안의 권한 (일반적) |
| ClusterRole / ClusterRoleBinding | 클러스터 전체 | 노드·PV처럼 네임스페이스가 없는 리소스, 또는 전 네임스페이스 권한 |

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pod-reader
  namespace: demo
rules:
  - apiGroups: [""]            # ""는 core 그룹 (pods, services, configmaps...)
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: dev-team-pod-reader
  namespace: demo
subjects:
  - kind: User
    name: dev-user
    apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: Role
  name: pod-reader
  apiGroup: rbac.authorization.k8s.io
```

이 선언의 의미: `dev-user`는 `demo` 네임스페이스의 Pod를 조회하고 로그를 볼 수 있지만, 삭제·수정은 못 하고 다른 네임스페이스는 보이지도 않습니다. RBAC에는 거부 규칙이 없다는 점이 중요합니다 — **기본이 전부 거부이고, Role은 허용만 추가**합니다. 그래서 설계는 "필요한 최소한만 허용"이라는 한 방향으로만 흐릅니다.

권한이 실제로 어떻게 걸려 있는지는 명령으로 확인합니다.

```bash
kubectl auth can-i delete pods -n demo                # 내 권한 확인 (yes/no)
kubectl auth can-i list secrets --as=dev-user -n demo # 다른 사용자 입장에서 확인
kubectl describe role pod-reader -n demo
```

## ServiceAccount: Pod의 신원

사람은 kubeconfig로 인증하지만, Pod 안에서 도는 앱이 API를 호출할 때는 ServiceAccount(SA)로 인증합니다. 모든 Pod는 SA를 하나 달고 뜨며, 지정하지 않으면 네임스페이스의 `default` SA가 붙습니다.

API 접근이 필요한 앱(예: 같은 네임스페이스의 Pod 목록을 읽는 운영 도구)에는 전용 SA를 만들어 필요한 Role만 묶어 줍니다.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: pod-monitor
  namespace: demo
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: pod-monitor-reader
  namespace: demo
subjects:
  - kind: ServiceAccount
    name: pod-monitor
    namespace: demo
roleRef:
  kind: Role
  name: pod-reader
  apiGroup: rbac.authorization.k8s.io
```

```yaml
# Deployment의 Pod 템플릿에서 SA 지정
spec:
  template:
    spec:
      serviceAccountName: pod-monitor
      containers:
        - name: monitor
          image: my-monitor:1.0
```

SA 토큰은 컨테이너 안 `/var/run/secrets/kubernetes.io/serviceaccount/`에 자동 마운트됩니다. 뒤집어 말하면, API를 쓰지 않는 보통의 web/WAS에도 default SA 토큰이 들어가 있다는 뜻입니다. 컨테이너가 뚫렸을 때 공격자에게 API 토큰을 쥐여줄 이유가 없으므로, API가 필요 없는 워크로드에는 `automountServiceAccountToken: false`를 선언해 마운트 자체를 끄는 것이 좋은 기본값입니다.

## SecurityContext: 컨테이너의 실행 권한

RBAC가 "API에 대한 권한"이라면 SecurityContext는 "노드 위에서의 권한"입니다. 컨테이너가 어떤 UID로 돌고, 루트 파일시스템에 쓸 수 있고, 커널 권한(capability)을 얼마나 갖는지를 선언합니다. 기본값은 관대한 편이라(이미지가 정한 사용자, 보통 root) 운영 워크로드에는 조여서 시작하는 것이 원칙입니다.

```yaml
spec:
  template:
    spec:
      securityContext:               # Pod 수준 — 모든 컨테이너에 적용
        runAsNonRoot: true           # root(UID 0) 실행이면 기동 거부
        runAsUser: 1000
        fsGroup: 2000                # 마운트된 볼륨의 그룹 소유권
      containers:
        - name: was
          image: my-was:1.0
          securityContext:           # 컨테이너 수준 — Pod 수준을 덮어씀
            allowPrivilegeEscalation: false   # setuid 등으로 권한 상승 금지
            readOnlyRootFilesystem: true      # 루트 FS 쓰기 금지
            capabilities:
              drop: ["ALL"]          # 커널 권한 전부 제거 후 필요한 것만 add
```

이 조합이 사실상의 권장 기본형입니다. `readOnlyRootFilesystem`을 켜면 앱이 임시 파일을 쓰는 경로(예: `/tmp`)가 막히는데, 그 경로만 [emptyDir 볼륨](04-config-and-volume.md)으로 뚫어 주면 됩니다. 80 포트처럼 1024 미만 포트 바인딩이 필요해 root가 필요해 보이는 경우도, 컨테이너 안에서는 8080으로 듣고 [Service](03-pod-deployment-service.md)에서 80 → 8080으로 연결하면 non-root를 유지할 수 있습니다.

적용 후에는 의도대로 도는지 확인합니다.

```bash
kubectl exec deploy/was -- id                 # uid=1000 확인
kubectl exec deploy/was -- touch /test        # Read-only file system 오류가 정상
```

세 도구의 관계를 한 줄로 정리하면 — **RBAC는 사람의 API 권한, ServiceAccount + RBAC는 앱의 API 권한, SecurityContext는 앱의 OS 권한**을 각각 최소로 조이는 장치입니다.

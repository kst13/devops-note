# kubectl 치트시트

일상 운영에서 손에 붙어야 하는 kubectl 명령을 조회, 디버깅, 리소스 조작 순으로 정리합니다. 명령 자체보다 "어떤 상황에서 어떤 순서로 쓰는가"를 기준으로 묶었습니다.

## 컨텍스트와 클러스터

kubeconfig에는 여러 클러스터의 접속 정보(컨텍스트)가 공존할 수 있습니다. 지금 어느 클러스터를 향해 명령을 치고 있는지 확인하는 것이 모든 작업의 출발점입니다.

```bash
kubectl config current-context        # 지금 바라보는 클러스터
kubectl config get-contexts           # 등록된 전체 컨텍스트
kubectl config use-context k3d-devops-note   # 컨텍스트 전환
kubectl cluster-info                  # API 서버 주소 확인
```

운영 클러스터와 실습 클러스터를 오갈 때 이 확인을 생략하면, 실습 명령이 운영으로 날아가는 사고가 납니다.

## 조회

```bash
kubectl get pods                      # 현재 네임스페이스의 Pod
kubectl get pods -A                   # 모든 네임스페이스
kubectl get pods -o wide              # 어느 노드에 떴는지, Pod IP까지
kubectl get pods --watch              # 상태 변화를 실시간 관찰
kubectl get deploy,svc,pvc            # 여러 종류를 한 번에
kubectl get pod web -o yaml           # 오브젝트 전체 정의 (현재 상태 포함)
kubectl describe pod web              # 사람이 읽기 좋은 요약 + Events
kubectl get events --sort-by=.lastTimestamp   # 최근 이벤트 시간순
```

`describe` 출력 맨 아래의 Events가 문제 해결의 단서 대부분을 담고 있습니다. 이미지 풀 실패, 스케줄 불가, 프로브 실패가 전부 여기 기록됩니다.

## 로그와 디버깅

```bash
kubectl logs web-abc123               # Pod 로그
kubectl logs -f web-abc123            # 실시간 팔로우
kubectl logs web-abc123 --previous    # 재시작 전 컨테이너의 로그 (크래시 원인 확인)
kubectl logs -l app=web               # 라벨로 여러 Pod의 로그를 한 번에
kubectl exec -it web-abc123 -- sh     # 컨테이너 안에서 셸 실행
kubectl port-forward svc/web 8080:80  # 로컬 포트를 클러스터 내부로 연결
kubectl debug -it web-abc123 --image=busybox   # 셸이 없는 컨테이너에 디버그 컨테이너 부착
```

`--previous`는 CrashLoopBackOff 조사에서 가장 먼저 칠 명령입니다. 지금 컨테이너는 아직 죽지 않았거나 로그가 비어 있는 경우가 많고, 원인은 직전에 죽은 컨테이너의 마지막 로그에 있습니다.

## 리소스 조작

```bash
kubectl apply -f deployment.yaml      # 선언 적용 (생성이든 수정이든 동일)
kubectl delete -f deployment.yaml     # 선언한 리소스 삭제
kubectl scale deployment web --replicas=4
kubectl rollout status deployment/web    # 배포 진행 상황
kubectl rollout history deployment/web   # 배포 이력
kubectl rollout undo deployment/web      # 직전 버전으로 롤백
kubectl rollout restart deployment/web   # 설정 변경 반영을 위한 롤링 재시작
```

`kubectl edit`(라이브 오브젝트를 편집기로 수정)도 존재하지만 지양합니다. 편집한 내용이 YAML 파일에 남지 않아, 다음 `apply` 때 조용히 되돌아가고 변경 이력도 사라집니다. 수정은 파일을 고쳐 `apply` 하는 것을 원칙으로 삼아야 Git 기반 운영(GitOps)으로 자연스럽게 이어집니다.

## 자주 쓰는 진단 순서

Pod가 안 뜰 때 기계적으로 밟는 순서입니다.

1. `kubectl get pods` — 상태 확인 (Pending, ImagePullBackOff, CrashLoopBackOff 구분)
2. `kubectl describe pod <이름>` — Events에서 원인 단서 확인
3. Pending이면 스케줄 문제 (리소스 부족, 노드 셀렉터), ImagePullBackOff면 이미지 이름·레지스트리 인증 확인
4. CrashLoopBackOff면 `kubectl logs <이름> --previous`로 죽기 직전 로그 확인
5. 떴는데 응답이 없으면 `kubectl port-forward`로 Pod에 직접 붙어 앱 문제인지 Service 문제인지 분리

## 노드 점검과 배출

서버 점검(OS 패치, 하드웨어 교체)으로 노드를 내릴 때는 그 위의 Pod를 먼저 안전하게 비웁니다.

```bash
kubectl cordon node-2       # 새 Pod 배치만 차단 (기존 Pod는 유지)
kubectl drain node-2 --ignore-daemonsets --delete-emptydir-data   # 기존 Pod 배출
# ... 노드 점검 작업 ...
kubectl uncordon node-2     # 점검 후 배치 허용으로 복귀
```

drain은 Pod를 다른 노드로 "옮기는" 것이 아니라 종료시키는 것입니다. Deployment가 다른 노드에 새 Pod를 다시 띄워 주는 것이므로, replicas가 1인 워크로드는 drain 중 그 서비스가 끊깁니다. 점검 전에 replicas가 2 이상인지, [분산 배치](../concepts/10-web-was-workload-design.md)가 되어 있는지 확인하는 이유입니다. `--ignore-daemonsets`는 노드마다 반드시 하나씩 떠야 하는 DaemonSet Pod(로그 수집기 등)는 배출 대상에서 제외한다는 뜻이고, drain이 이 옵션 없이는 거부되므로 사실상 항상 붙입니다. 점검이 끝나면 uncordon을 잊지 않아야 합니다 — cordon 상태가 남아 있으면 그 노드는 계속 비어 있게 됩니다.

## 다음 도구: k9s

kubectl이 손에 붙었다면 k9s를 쓸 차례입니다. k9s는 터미널에서 클러스터를 실시간으로 탐색하는 TUI 도구로, 위에서 정리한 조회·로그·exec 흐름을 키 입력 몇 번으로 줄여 줍니다. Pod 목록이 실시간 갱신되는 화면에서 `l`로 로그, `s`로 셸, `d`로 describe를 열고, `:svc` `:deploy`처럼 vim 스타일로 화면을 전환합니다.

```bash
brew install k9s
k9s               # 현재 kubeconfig 컨텍스트로 접속
```

순서가 중요합니다. kubectl로 각 명령이 무엇을 하는지 먼저 이해해야 k9s가 보여 주는 화면과 단축키의 의미가 잡힙니다. 조회·디버깅은 k9s로, 변경은 YAML 파일과 `kubectl apply`로 — 이 분리가 일상 운영의 기본형입니다.

# NetworkPolicy로 Pod 트래픽 제한

쿠버네티스의 Pod 네트워크는 기본이 전면 허용입니다 — 어느 네임스페이스의 어떤 Pod든 다른 모든 Pod에 접속할 수 있습니다. NetworkPolicy는 이 평면에 "누가 누구와 통신할 수 있는가"라는 방화벽 규칙을 선언하는 리소스입니다. [web/WAS 구조](10-web-was-workload-design.md)에서 "WAS는 web에서만, DB는 WAS에서만 접근 가능"을 강제하는 도구입니다.

> **CKAD 시험 범위** — Services and Networking(20%) 도메인의 단골 주제입니다. "특정 Pod·네임스페이스에서만 접근 허용" 유형이 거의 매 시험 출제됩니다.

## 동작 원리: 선택되는 순간 기본 거부로 바뀐다

NetworkPolicy의 핵심 규칙은 하나입니다. **어떤 정책의 `podSelector`에 선택된 Pod는, 그 방향(ingress 또는 egress)에 대해 "정책들이 허용한 트래픽만" 받게 됩니다.** 선택되지 않은 Pod는 여전히 전면 허용입니다.

즉 거부 규칙을 쓰는 것이 아니라, RBAC와 같은 방식으로 — 선택해서 기본 거부로 만든 뒤 허용을 추가하는 구조입니다. 정책 여러 개가 같은 Pod를 선택하면 허용 목록이 합쳐집니다.

## 구성 요소

| 필드 | 의미 |
| --- | --- |
| `podSelector` | 이 정책이 적용될 대상 Pod (라벨 기준, 정책이 있는 네임스페이스 안에서) |
| `policyTypes` | 제한할 방향 — `Ingress`(들어오는 것), `Egress`(나가는 것) |
| `ingress.from` / `egress.to` | 허용할 상대 — `podSelector`, `namespaceSelector`, `ipBlock` 조합 |
| `ports` | 허용할 프로토콜·포트 |

## 예시: WAS는 web에서만 접근 허용

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: was-allow-from-web
  namespace: demo
spec:
  podSelector:
    matchLabels:
      app: was               # 이 정책의 보호 대상: WAS Pod
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: web       # 같은 네임스페이스의 web Pod만 허용
      ports:
        - protocol: TCP
          port: 8080
```

적용하는 순간 `app: was` Pod는 ingress 기본 거부가 되고, 목록에 있는 "web Pod가 8080으로"만 통과합니다. 다른 네임스페이스의 Pod도, 같은 네임스페이스의 다른 앱도 WAS에 접속할 수 없게 됩니다.

`from` 항목을 쓸 때 주의할 문법 함정 하나 — `namespaceSelector`와 `podSelector`를 **한 항목 안에 같이** 쓰면 AND(그 네임스페이스의 그 Pod), **별도 항목으로 나눠** 쓰면 OR(그 네임스페이스 전체 또는 그 Pod)입니다. 들여쓰기 한 칸(`-` 유무) 차이로 의미가 완전히 달라지므로 적용 후 반드시 실제 통신으로 검증합니다.

## 기본 거부 정책부터 까는 전략

보안이 목적이라면 네임스페이스에 "전부 거부"를 먼저 깔고 필요한 허용을 하나씩 얹는 순서가 정석입니다.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-ingress
  namespace: demo
spec:
  podSelector: {}            # 빈 셀렉터 = 네임스페이스의 모든 Pod 선택
  policyTypes:
    - Ingress
```

`podSelector: {}`는 "모든 Pod를 선택"한다는 뜻이므로, 이 정책 하나로 demo 네임스페이스 전체가 ingress 기본 거부가 됩니다. egress까지 잠글 때는 DNS(53/UDP, kube-system의 CoreDNS)를 허용하는 것을 잊으면 안 됩니다 — 이름 해석이 막혀 "네트워크 전체가 죽은 것 같은" 증상이 납니다.

## 검증

정책은 선언이 받아들여졌다고 동작이 보장되는 게 아니므로(아래 CNI 참고), 실제 통신으로 확인합니다.

```bash
# 허용된 경로: web Pod에서 WAS로 — 성공해야 함
kubectl exec deploy/web -n demo -- curl -s --max-time 3 http://was:8080/

# 차단된 경로: 임시 Pod에서 WAS로 — 타임아웃이 정상
kubectl run test --rm -it --image=busybox:1.36 -n demo \
  -- wget -qO- --timeout=3 http://was:8080/
```

## CNI가 지원해야 동작한다

NetworkPolicy는 API 리소스일 뿐, 실제 집행은 [CNI 플러그인](02-ways-to-run-kubernetes.md)의 몫입니다. 지원하지 않는 CNI에서는 **정책을 적용해도 아무 오류 없이 그냥 무시됩니다** — 가장 위험한 실패 형태입니다. 표준 Flannel 단독 구성이 대표적인 미지원 사례인데, k3s/k3d는 Flannel을 쓰면서도 자체 네트워크 정책 컨트롤러를 내장하고 있어 NetworkPolicy가 동작합니다. 이 시리즈의 실습 환경(k3d)과 실서버(k3s), 그리고 RKE2(Canal/Cilium) 모두 해당되므로 안심하고 실습해도 되지만, 다른 환경으로 옮길 때는 CNI의 지원 여부를 반드시 확인하고 위 검증 절차를 다시 돌립니다.

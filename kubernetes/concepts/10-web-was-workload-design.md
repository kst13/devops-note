# web/WAS 워크로드 설계

web과 WAS를 쿠버네티스에 올릴 때 [기본 Deployment](03-pod-deployment-service.md)에 더해 실제 운영 트래픽을 받기 위해 필요한 설정들을 다룹니다. 핵심은 네 가지입니다 — 트래픽을 받아도 되는 시점을 알리는 probe, 배포 중에도 요청을 잃지 않는 롤링 전략, JVM과 리소스 제한의 관계, 그리고 세션 처리입니다.

## 전체 구조

[Ingress 라우팅 예제](../examples/ingress-routing/README.md)의 2계층 구조가 출발점입니다. web(정적 자원·리버스 프록시)과 WAS(애플리케이션)를 각각 Deployment + Service로 선언하고, 외부 노출은 web만 Ingress로 합니다. WAS는 ClusterIP로 클러스터 내부에만 열어 두는 것이 기본형입니다.

```text
사용자 → [진입점] → Ingress → web Service → web Pod (N개)
                                              └→ was Service → was Pod (M개)
```

web과 WAS의 대수(replicas)는 서로 독립적으로 조절합니다. 이것이 계층을 나누는 실익 중 하나입니다.

## Probe: 트래픽을 받아도 되는가

쿠버네티스는 컨테이너 프로세스가 떠 있으면 Pod를 Ready로 취급하고 Service 대상에 넣습니다. 문제는 WAS입니다. Tomcat 프로세스는 떴지만 애플리케이션 초기화가 30초 더 걸린다면, 그 30초 동안 들어온 요청은 전부 실패합니다. 이를 막는 것이 probe입니다.

```yaml
containers:
  - name: was
    image: my-was:1.0
    ports:
      - containerPort: 8080
    # 준비될 때까지 Service 대상에서 제외 — 무중단 배포의 전제 조건
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8080
      periodSeconds: 5
    # 애플리케이션이 데드락 등으로 멈추면 컨테이너 재시작
    livenessProbe:
      httpGet:
        path: /health/live
        port: 8080
      periodSeconds: 10
      failureThreshold: 3
    # 기동이 느린 WAS를 위해 liveness 판정을 기동 완료까지 유예
    startupProbe:
      httpGet:
        path: /health/ready
        port: 8080
      periodSeconds: 5
      failureThreshold: 60   # 최대 300초까지 기동 대기
```

세 probe의 역할 구분이 중요합니다. readiness는 "지금 트래픽을 받아도 되는가"(실패해도 재시작하지 않고 Service에서만 빠짐), liveness는 "프로세스가 회복 불능인가"(실패하면 재시작), startup은 "기동이 끝났는가"(끝나기 전까지 liveness를 유예)입니다. liveness를 readiness처럼 민감하게 잡으면 일시적 과부하 때 멀쩡한 WAS를 재시작해 장애를 증폭시키므로, liveness는 보수적으로 설정합니다. probe 경로는 DB 연결까지 확인하는 무거운 체크가 아니라 애플리케이션 자체의 상태만 보는 가벼운 엔드포인트로 만듭니다.

## 무중단 배포

Deployment의 롤링 업데이트는 readiness와 종료 처리가 갖춰져야 실제로 무중단이 됩니다.

```yaml
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1          # 새 Pod를 1개 더 띄운 뒤
      maxUnavailable: 0    # 기존 Pod는 새 Pod가 Ready된 후에만 제거
  template:
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: was
          lifecycle:
            preStop:
              exec:
                command: ["sleep", "10"]
```

새 Pod 쪽은 readiness가 지켜 줍니다(Ready 전에는 트래픽이 안 옴). 문제는 죽는 Pod 쪽입니다. 쿠버네티스가 Pod를 Service에서 빼는 것과 종료 신호(SIGTERM)를 보내는 것은 동시에 일어나서, 빼는 작업이 전파되기 전 몇 초간 트래픽이 죽어가는 Pod로 갈 수 있습니다. `preStop`의 짧은 sleep이 이 전파 시간을 벌어 주고, 그 후 SIGTERM을 받은 WAS가 처리 중인 요청을 마치도록 graceful shutdown(Spring Boot의 `server.shutdown=graceful` 등)을 설정하며, `terminationGracePeriodSeconds`는 그 전체 시간보다 길게 잡습니다.

## 리소스와 JVM

requests와 limits는 WAS에서 특히 중요합니다. JVM 힙과의 관계 때문입니다.

```yaml
containers:
  - name: was
    resources:
      requests:            # 스케줄러가 배치를 결정하는 기준
        cpu: "500m"
        memory: "1Gi"
      limits:              # 초과 시 제재 기준
        memory: "1Gi"      # 초과하면 OOMKilled
    env:
      - name: JAVA_TOOL_OPTIONS
        value: "-XX:MaxRAMPercentage=75.0"
```

memory limit을 넘으면 컨테이너는 경고 없이 OOMKilled로 죽습니다. JVM은 힙 외에도 메타스페이스, 스레드 스택, 네이티브 메모리를 쓰므로, 힙 최대치를 limit의 70~75% 수준으로 잡는 것이 출발점입니다. 최신 JVM은 컨테이너의 메모리 limit을 인식하므로 `-Xmx`로 절대값을 박는 대신 `MaxRAMPercentage`로 비율을 지정하면 limit 변경 시 힙이 따라갑니다. CPU limit은 걸지 않는 선택도 흔합니다 — CPU는 초과해도 죽지 않고 스로틀만 되는데, JVM의 GC 스레드가 스로틀되면 지연이 튀기 때문에 requests만 두고 limit은 생략하는 것입니다. 반복되는 재시작의 원인이 `kubectl describe pod`에서 OOMKilled로 나오면 이 절을 다시 봅니다.

## 세션 처리

replicas가 2 이상이 되는 순간 세션 문제가 생깁니다. 로그인 세션이 WAS-1 메모리에 있는데 다음 요청이 WAS-2로 가면 로그아웃되는 문제입니다. 해법은 두 가지입니다.

| 방식 | 동작 | 평가 |
| --- | --- | --- |
| Sticky session | Ingress가 쿠키로 같은 사용자를 같은 Pod에 고정 | 설정만으로 가능하지만 Pod가 죽거나 배포로 교체되면 그 사용자 세션은 소실 |
| 외부 세션 저장소 | 세션을 Redis 등 외부에 저장, WAS는 무상태화 | Pod 교체·스케일과 무관하게 세션 유지 — 쿠버네티스 환경의 정석 |

추천은 외부 세션 저장소입니다. 롤링 업데이트는 Pod를 반드시 교체하므로, sticky session만으로는 "배포할 때마다 일부 사용자 로그아웃"을 피할 수 없습니다. Spring Session + Redis 조합이 대표적이며, Redis 구성은 [redis 토픽](../../redis/README.md)을 참고합니다. sticky가 필요한 과도기에는 Traefik의 Service 어노테이션으로 쿠키 기반 고정을 걸 수 있습니다.

## 3대 구성에서의 배치 분산

[소규모 절충 구성](07-production-cluster-topology.md)(3대 겸임)에서 replicas 3이 우연히 한두 노드에 몰리면 노드 장애 때 여러 대가 한꺼번에 죽습니다. 같은 앱의 Pod를 노드별로 고르게 퍼뜨리도록 선언해 둡니다.

```yaml
spec:
  template:
    spec:
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              app: was
```

`whenUnsatisfiable: ScheduleAnyway`는 "가능하면 분산하되, 불가능한 상황(노드 부족)에서도 배치는 한다"는 뜻입니다. 3대 규모에서는 엄격 모드(`DoNotSchedule`)보다 이쪽이 운영이 순합니다.

노드를 점검으로 내릴 때 Pod를 안전하게 비우는 절차(cordon/drain)는 [kubectl 치트시트](../commands/kubectl-cheatsheet.md)에 정리되어 있습니다.

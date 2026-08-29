# Job, CronJob과 멀티 컨테이너 Pod

Deployment는 "계속 떠 있는 서비스"를 위한 워크로드입니다. 이 문서는 그 밖의 실행 형태를 다룹니다 — 한 번 실행하고 끝나는 작업(Job), 주기적으로 실행되는 작업(CronJob), 그리고 Pod 하나에 컨테이너를 여러 개 두는 패턴(init, sidecar)입니다. CKAD 시험의 단골 출제 영역이기도 합니다.

## 워크로드 리소스 고르기

"어떤 리소스로 실행할 것인가"는 프로세스의 수명이 결정합니다.

| 리소스 | 수명 | 용도 |
| --- | --- | --- |
| Deployment | 계속 떠 있음, 죽으면 재시작 | web, WAS, API 서버 |
| Job | 완료될 때까지 실행되고 끝남 | DB 마이그레이션, 배치 처리, 일회성 스크립트 |
| CronJob | 스케줄마다 Job을 생성 | 정기 백업, 리포트 생성, 정리 작업 |
| DaemonSet | 모든(또는 선택된) 노드에 1개씩 | 로그 수집기, 노드 모니터링 에이전트 |
| StatefulSet | 고정된 이름·저장소를 가진 Pod 집합 | DB처럼 각 인스턴스의 정체성이 필요한 워크로드 |

Deployment로 배치 작업을 돌리면 작업이 끝나도 컨테이너를 계속 재시작하는 문제가 생깁니다. "끝나는 것이 정상"인 작업은 Job이 맞는 도구입니다.

## Job: 완료가 목표인 워크로드

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migration
spec:
  backoffLimit: 3          # 실패 시 최대 3번까지 재시도
  ttlSecondsAfterFinished: 3600   # 완료 1시간 뒤 Job과 Pod 자동 정리
  template:
    spec:
      restartPolicy: Never # Job에서는 Always 불가 — Never 또는 OnFailure
      containers:
        - name: migrate
          image: my-app:1.0
          command: ["python", "manage.py", "migrate"]
```

Deployment와 다른 지점 두 가지에 주의합니다. `restartPolicy`는 반드시 `Never` 또는 `OnFailure`로 지정해야 하고(기본값 Always는 Job에서 거부됨), 완료된 Job은 저절로 사라지지 않으므로 `ttlSecondsAfterFinished`로 정리를 선언해 두는 것이 좋습니다.

여러 개를 처리하는 배치라면 `completions`(총 몇 번 완료돼야 하는가)와 `parallelism`(동시에 몇 개 실행하는가)을 조합합니다.

```bash
kubectl apply -f job.yaml
kubectl get jobs                    # COMPLETIONS 진행 상황
kubectl logs job/db-migration       # Job 이름으로 바로 로그 조회
```

## CronJob: 스케줄마다 Job 생성

CronJob은 Job을 감싼 리소스입니다. 스케줄이 되면 위에서 본 Job을 하나 만들어 실행합니다.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-report
spec:
  schedule: "0 2 * * *"        # 매일 02:00 (cron 5필드)
  timeZone: "Asia/Seoul"       # 지정하지 않으면 UTC 기준 — 흔한 함정
  concurrencyPolicy: Forbid    # 이전 실행이 안 끝났으면 이번 회차 건너뜀
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: report
              image: my-app:1.0
              command: ["python", "make_report.py"]
```

운영에서 자주 쓰는 조작 두 가지: 점검 기간에 일시 정지하려면 `kubectl patch cronjob nightly-report -p '{"spec":{"suspend":true}}'`, 스케줄을 기다리지 않고 즉시 한 번 돌려 보려면 `kubectl create job test-run --from=cronjob/nightly-report`.

## init 컨테이너: 본 컨테이너 전에 준비 작업

init 컨테이너는 본 컨테이너가 시작되기 전에 **순서대로, 각각 완료될 때까지** 실행됩니다. "DB가 뜰 때까지 대기", "설정 파일 다운로드" 같은 준비 작업을 앱 이미지에 섞지 않고 분리하는 도구입니다.

```yaml
spec:
  initContainers:
    - name: wait-for-db
      image: busybox:1.36
      command: ["sh", "-c", "until nc -z db-service 5432; do sleep 2; done"]
  containers:
    - name: was
      image: my-was:1.0
```

init 컨테이너가 실패하면 Pod는 본 컨테이너를 시작하지 않고 init부터 재시도합니다. `kubectl get pods`에서 `Init:0/1` 상태로 멈춰 있다면 init 컨테이너의 로그(`kubectl logs <pod> -c wait-for-db`)를 봅니다.

## sidecar 컨테이너: 본 컨테이너와 나란히 보조 실행

sidecar는 앱 컨테이너 옆에서 같은 Pod의 네트워크·볼륨을 공유하며 계속 실행되는 보조 컨테이너입니다. 대표 사례가 로그 수집입니다 — 앱은 파일에 로그를 쓰고, sidecar가 그 파일을 읽어 수집 서버로 보냅니다.

Kubernetes 1.29부터는 sidecar를 `initContainers`에 `restartPolicy: Always`를 붙여 선언하는 것이 표준입니다. 이렇게 하면 "본 컨테이너보다 먼저 시작하고, 본 컨테이너가 끝난 뒤 종료되는" 순서가 보장됩니다.

```yaml
spec:
  initContainers:
    - name: log-shipper
      image: fluent-bit:3.0
      restartPolicy: Always      # 이 한 줄이 init을 sidecar로 만든다
      volumeMounts:
        - name: app-logs
          mountPath: /var/log/app
  containers:
    - name: was
      image: my-was:1.0
      volumeMounts:
        - name: app-logs
          mountPath: /var/log/app
  volumes:
    - name: app-logs
      emptyDir: {}
```

두 컨테이너가 [emptyDir 볼륨](04-config-and-volume.md)을 공유하는 것이 연결 고리입니다. 같은 Pod이므로 네트워크도 공유해서, sidecar가 `localhost`로 앱에 접근할 수도 있습니다(프록시 패턴).

패턴을 정리하면 — **init은 "먼저 실행되고 끝나는" 준비 작업, sidecar는 "나란히 계속 도는" 보조 작업**입니다. 어느 쪽이든 "Pod는 함께 배치되어야 하는 컨테이너 묶음"이라는 [기본 개념](03-pod-deployment-service.md)의 실전 활용입니다.

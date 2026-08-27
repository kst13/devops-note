# 스토리지와 로그

Docker에서는 호스트 디렉터리를 컨테이너에 bind mount하고 컨테이너 로그를 호스트 파일로
저장하는 방식을 자주 사용합니다. Kubernetes에서는 파일 데이터의 수명과 로그의 수집
경로를 분리해 설계합니다.

```text
파일 데이터 → PVC 중심
로그        → stdout/stderr 출력 후 중앙 수집
```

## Docker 볼륨과 Kubernetes 스토리지 비교

| Docker | Kubernetes | 용도와 주의점 |
| --- | --- | --- |
| `-v /srv/app:/app/data` | PVC | 애플리케이션 데이터 보존 |
| Docker volume | PersistentVolume/PVC | 클러스터가 관리하는 스토리지 |
| 컨테이너 임시 파일 | `emptyDir` | Pod가 삭제되면 함께 삭제 |
| 호스트 경로 직접 마운트 | `hostPath` | 특정 노드에 종속되므로 제한적으로 사용 |

## PVC 사용

PVC(PersistentVolumeClaim)는 애플리케이션이 필요한 스토리지를 요청하는 객체입니다.
Pod가 재생성되어도 PVC가 유지되는 한 데이터를 다시 마운트할 수 있습니다.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: app-data
  namespace: demo
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: demo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: app
  template:
    metadata:
      labels:
        app: app
    spec:
      containers:
        - name: app
          image: nginx:1.28-alpine
          volumeMounts:
            - name: app-data
              mountPath: /var/lib/app
      volumes:
        - name: app-data
          persistentVolumeClaim:
            claimName: app-data
```

```bash
kubectl apply -f storage.yaml
kubectl get pvc,pv -n demo
```

`storageClassName`을 생략하면 클러스터의 기본 StorageClass가 사용됩니다.

## k3d의 local-path 주의점

k3d/k3s의 기본 StorageClass인 `local-path`는 노드 로컬 디스크를 사용합니다.
일반적으로 데이터는 다음 경로에 저장됩니다.

```text
/var/lib/rancher/k3s/storage
```

k3d에서는 이 경로가 agent/server 컨테이너 내부에 있으므로, 클러스터를 삭제하면 PVC
데이터도 함께 사라질 수 있습니다.

호스트 PC에 실습 데이터를 남기려면 클러스터 생성 시 디렉터리를 매핑합니다.

```bash
mkdir -p "$PWD/k3d-storage"

k3d cluster create lab \
  --agents 2 \
  -v "$PWD/k3d-storage:/var/lib/rancher/k3s/storage@all" \
  -p "8080:80@loadbalancer"
```

`local-path`는 볼륨이 특정 노드에 묶이는 방식이므로, 노드 장애나 Pod 이동을 자동으로
해결하는 공유 스토리지가 아닙니다. 운영에서는 Longhorn, Ceph, 클라우드 디스크 CSI 등
환경에 맞는 스토리지를 검토합니다.

## hostPath의 한계

`hostPath`는 Docker bind mount와 비슷하게 호스트 경로를 직접 마운트합니다.

```yaml
volumes:
  - name: host-data
    hostPath:
      path: /srv/app/data
      type: DirectoryOrCreate
```

그러나 Pod가 다른 노드로 이동하면 같은 경로에 기존 데이터가 없을 수 있습니다.
노드별 시스템 디렉터리 접근 등 특별한 목적이 아니라면 애플리케이션 데이터에는 PVC를
사용합니다.

## 로그는 stdout/stderr가 기본

Kubernetes에서는 애플리케이션이 로그 파일보다 표준 출력과 표준 에러로 로그를 내보내는
방식을 권장합니다.

```text
애플리케이션
  → stdout/stderr
  → containerd/kubelet
  → kubectl logs 또는 로그 수집기
```

조회 명령:

```bash
kubectl logs -n demo deployment/app
kubectl logs -f -n demo deployment/app
kubectl get pods -n demo -l app=app
kubectl logs -n demo <pod-name>
```

공식 `nginx` 이미지는 access/error 로그를 컨테이너 로그로 출력하므로 별도 파일 마운트
없이 `kubectl logs`로 확인할 수 있습니다.

## 파일 로그가 필요한 레거시 애플리케이션

애플리케이션이 반드시 파일에 로그를 기록해야 한다면 PVC를 로그 디렉터리에 마운트할 수
있습니다.

```yaml
volumeMounts:
  - name: app-logs
    mountPath: /var/log/my-app
volumes:
  - name: app-logs
    persistentVolumeClaim:
      claimName: app-logs
```

이 방식은 파일 보존은 가능하지만 검색·집계·알림을 별도로 구성해야 합니다. 가능하면
애플리케이션 로그를 stdout/stderr로 전환하고, 변경이 어려운 경우에만 sidecar가 같은
볼륨의 파일을 읽어 stdout으로 내보내도록 구성합니다.

## 중앙 로그 수집

운영에서는 각 노드의 로그 파일을 직접 수동으로 읽기보다 수집기를 배치합니다.

```text
애플리케이션 stdout/stderr
  → Fluent Bit 또는 Vector
  → Loki 또는 Elasticsearch
  → Grafana 또는 Kibana
```

이렇게 하면 Pod가 재생성되거나 노드가 교체되어도 로그를 한 곳에서 검색할 수 있습니다.

## 수명과 장애 범위

```text
emptyDir  → Pod 삭제 시 삭제
PVC       → Pod 삭제 후에도 유지(스토리지 정책에 따름)
hostPath  → 노드에 종속
로그 파일  → 수집하지 않으면 Pod/노드 교체 시 잃을 수 있음
```

특히 `kubectl delete pod`와 `kubectl delete pvc`는 영향 범위가 다릅니다. PVC를 삭제하면
StorageClass의 reclaim 정책(`Delete` 또는 `Retain`)에 따라 실제 PV 데이터가 삭제될 수
있으므로, 운영 데이터는 삭제 전에 백업과 reclaim 정책을 확인합니다.

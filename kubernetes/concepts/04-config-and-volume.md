# 설정과 볼륨

설정을 컨테이너 이미지에 굽지 않고 분리하는 방법(ConfigMap, Secret)과 컨테이너가 사라져도 데이터를 유지하는 방법(볼륨, PVC)을 다룹니다. 같은 이미지를 개발·스테이지·운영에 그대로 쓰고 설정만 바꿔 배포하는 것이 목표입니다. 실습은 [이전 문서](03-pod-deployment-service.md)의 k3d 클러스터(`devops-note`)를 이어서 사용합니다.

> **CKAD 시험 범위** — ConfigMap·Secret은 Application Environment, Configuration and Security(25%), 볼륨은 Application Design and Build(20%) 도메인에 해당합니다.

## ConfigMap

ConfigMap은 키-값 형태의 설정 묶음입니다. 명령으로 만들 수도, YAML로 선언할 수도 있습니다.

```bash
kubectl create configmap web-config --from-literal=APP_MODE=dev
```

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-config
data:
  APP_MODE: "dev"
  nginx.conf: |
    server {
      listen 80;
    }
```

주입 방법은 두 가지입니다. 환경 변수로 넣거나, 파일로 마운트합니다.

```yaml
# Deployment의 containers 항목에 추가하는 스니펫
containers:
  - name: web
    image: nginx:1.27-alpine
    envFrom:
      - configMapRef:
          name: web-config          # data의 모든 키가 환경 변수로 주입
    volumeMounts:
      - name: config
        mountPath: /etc/config      # data의 각 키가 이 경로의 파일이 됨
volumes:
  - name: config
    configMap:
      name: web-config
```

```bash
kubectl exec deploy/web -- env | grep APP_MODE
kubectl exec deploy/web -- cat /etc/config/nginx.conf
```

환경 변수 주입은 Pod 시작 시점에 고정되므로 ConfigMap을 바꿔도 재시작 전에는 반영되지 않습니다. 파일 마운트는 시간이 지나면 갱신되지만 앱이 파일을 다시 읽어야 합니다. 어느 쪽이든 "설정 변경 후 롤링 재시작(`kubectl rollout restart deploy/web`)"을 기본 절차로 삼는 것이 예측 가능합니다.

## Secret

Secret은 비밀번호, 토큰, 인증서처럼 노출되면 안 되는 값을 담는 오브젝트입니다. 사용법은 ConfigMap과 거의 같습니다.

```bash
kubectl create secret generic db-credentials \
  --from-literal=DB_USER=app \
  --from-literal=DB_PASSWORD=${DB_PASSWORD}
```

```yaml
# 환경 변수로 주입하는 스니펫
containers:
  - name: web
    image: nginx:1.27-alpine
    env:
      - name: DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: DB_PASSWORD
```

반드시 알아야 할 것: Secret의 값은 base64 인코딩일 뿐 암호화가 아닙니다. `kubectl get secret -o yaml`로 보면 누구나 디코딩할 수 있습니다. Secret이 ConfigMap과 다른 점은 접근 권한(RBAC)을 따로 좁힐 수 있고, etcd 저장 시 암호화를 켤 수 있는 대상이 된다는 것입니다. etcd 암호화 설정과 외부 시크릿 관리(Vault, External Secrets 등)는 별도 주제이며, 여기서는 "Secret을 YAML 파일에 실제 값으로 적어 Git에 커밋하면 안 된다"는 원칙만 지킵니다. 위 예시처럼 값은 항상 플레이스홀더나 환경 변수로 둡니다.

## 볼륨 기초

컨테이너의 파일시스템은 컨테이너가 사라지면 함께 사라집니다. 데이터를 유지하려면 볼륨을 붙여야 하고, 볼륨은 수명이 어디에 묶이는가로 구분하면 명확합니다.

| 종류 | 수명 | 용도 |
| --- | --- | --- |
| emptyDir | Pod와 함께 생성·삭제 | 같은 Pod 안 컨테이너 간 공유, 임시 캐시 |
| hostPath | 노드의 디스크 | 노드에 종속되므로 실습·특수 목적 외에는 비권장 |
| PersistentVolumeClaim (PVC) | 클러스터가 관리, Pod와 독립 | 데이터베이스 등 유지해야 하는 데이터의 표준 |

PVC는 "이만큼의 저장소가 필요하다"는 요청입니다. 실제 디스크를 어떻게 마련할지는 StorageClass가 결정합니다.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: web-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  # storageClassName을 생략하면 클러스터의 기본 StorageClass 사용
  # k3d/k3s에서는 local-path가 기본값
```

```yaml
# Deployment에 마운트하는 스니펫
containers:
  - name: web
    image: nginx:1.27-alpine
    volumeMounts:
      - name: data
        mountPath: /usr/share/nginx/html
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: web-data
```

```bash
kubectl apply -f pvc.yaml
kubectl get pvc,pv    # PVC가 Bound 상태이고 PV가 자동 생성되었는지 확인
```

이제 Pod를 지우고 다시 만들어도 `/usr/share/nginx/html`의 데이터는 유지됩니다. 데이터의 수명이 Pod가 아니라 PVC에 묶였기 때문입니다.

## StorageClass가 환경마다 다른 이유

PVC라는 요청 인터페이스는 어디서나 같지만, 그 요청을 실제 디스크로 바꾸는 구현은 환경마다 다릅니다. k3d/k3s는 노드의 로컬 디스크를 쓰는 local-path, EKS는 EBS 볼륨을 만드는 EBS CSI 드라이버, 온프레미스 표준 구성은 각자 선택한 스토리지 솔루션입니다. LoadBalancer와 마찬가지로 스토리지도 [환경의 가장자리](02-ways-to-run-kubernetes.md)에 속하므로, local-path에서 되던 것이 다른 환경에서 그대로 된다고 가정하지 않아야 합니다. 특히 local-path는 데이터가 특정 노드에 묶이므로 멀티 노드 운영에서는 제약을 이해하고 써야 합니다.

## 정리

```bash
kubectl delete pvc web-data
kubectl delete configmap web-config
kubectl delete secret db-credentials
```

여기까지가 로컬에서 익힐 워크로드의 기본기입니다. 다음 문서 [k3s 클러스터 설치](05-k3s-cluster-installation.md)에서 이 워크로드를 올릴 클러스터를 실서버에 직접 만듭니다.

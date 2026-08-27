# Kubernetes

쿠버네티스의 핵심 개념, 쿠버네티스를 경험하는 다양한 방법 비교, 로컬 k3d 실습, 실서버 k3s 클러스터 설치까지 학습 순서대로 정리합니다.

쿠버네티스는 어디서 실행하느냐(로컬 도구, 실서버 배포판, 관리형 서비스)에 따라 배우는 것이 달라집니다. 이 시리즈는 로컬 실습은 k3d(k3s in Docker), 실서버는 k3s를 기준으로 삼아 "배운 것을 그대로 실서버 운영까지" 이어가는 경로를 택합니다. API와 YAML은 어느 쿠버네티스에서든 동일하므로, 여기서 익힌 내용은 kind, EKS 같은 다른 환경에서도 그대로 통합니다.

## 문서 구조

```text
concepts/          핵심 개념과 실행 환경 비교, 실습, 실서버 설치
commands/          kubectl 조회, 디버깅, 리소스 조작 명령
```

## 추천 학습 순서

### 입문

1. [쿠버네티스 기본기](concepts/01-kubernetes-basics.md)
2. [쿠버네티스를 경험하는 방법들](concepts/02-ways-to-run-kubernetes.md)
3. [Pod, Deployment, Service](concepts/03-pod-deployment-service.md)
4. [설정과 볼륨](concepts/04-config-and-volume.md)
5. [kubectl 치트시트](commands/kubectl-cheatsheet.md)

### 구조 이해

6. [k3d 구조와 요청 경로](concepts/06-k3d-architecture-and-request-path.md)

### 실서버

7. [k3s 클러스터 설치](concepts/05-k3s-cluster-installation.md)
8. [실서버 클러스터 토폴로지](concepts/07-production-cluster-topology.md)

### 예제

- [도메인 기반 Ingress 라우팅](examples/ingress-routing/README.md)

### 명령 매뉴얼

- [k3d 실습 매뉴얼](commands/k3d-manual.md)

## 학습 기준

- 예제는 k3d(k3s in Docker)로 검증하며, 실서버 절차는 k3s 기준으로 작성합니다.
- YAML 매니페스트는 특정 배포판의 기본값에 기대지 않고 어느 환경에서도 동일하게 동작하도록 명시적으로 작성합니다.
- 도구 버전은 k3d v5, k3s v1.31 이상을 기준으로 하되, 사용 중인 버전의 명령과 옵션 지원 여부를 확인합니다.
- 컨테이너와 이미지 기초는 [docker 토픽](../docker/README.md)을 먼저 학습합니다.
- 조회와 디버깅은 kubectl로 익힌 뒤 k9s 같은 보조 도구로 넘어갑니다.

## 참고 기준

이 디렉터리는 2026-08-24 기준 쿠버네티스와 k3s 공식 문서를 우선 참고합니다. 관리형 서비스(EKS, GKE, AKS)는 지원 버전, 네트워크·스토리지 구현, 업그레이드 정책이 제품별로 다르므로 제공자 문서를 함께 확인합니다.

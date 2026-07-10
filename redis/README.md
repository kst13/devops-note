# Redis

Redis의 데이터 모델, 로컬 실습, 운영 설정, 복구, 고가용성, 트러블슈팅을 학습 순서대로 정리합니다.

Redis는 캐시, 세션, 큐, rate limit, Pub/Sub 등 여러 용도로 쓰이지만 목적에 따라 TTL, eviction, persistence, 복제 기준이 달라집니다. 각 문서는 설정값보다 먼저 사용 목적과 허용 가능한 데이터 유실 범위를 밝히는 것을 원칙으로 합니다.

## 문서 구조

```text
concepts/          데이터 모델과 운영 설계
examples/          직접 실행하는 최소 실습
commands/          Redis CLI와 운영 점검 명령
troubleshooting/   증상, 원인, 확인, 해결 기록
```

## 추천 학습 순서

### 입문

1. [Redis 데이터 모델과 만료](concepts/01-data-model-and-expiration.md)
2. [Redis 로컬 캐시 실습](examples/basic-cache/README.md)
3. [Redis CLI 운영 치트시트](commands/redis-cli-cheatsheet.md)

### 운영

4. [Redis 운영 설정 시작하기](concepts/02-production-configuration.md)
5. [Redis persistence, 백업, 복구](concepts/03-persistence-backup-and-recovery.md)
6. [Redis memory pressure와 OOM](troubleshooting/memory-pressure.md)

### 고가용성과 확장

7. [Redis 여러 대 구성 방식](concepts/04-multi-node.md)

## 학습 기준

- 예제는 Redis 8 공식 이미지로 검증하되, 운영에서는 사용 중인 버전의 명령과 설정 지원 여부를 확인합니다.
- 운영 keyspace는 `KEYS *` 대신 `SCAN` 계열 명령으로 조사합니다.
- 캐시와 임의 삭제가 불가능한 데이터를 같은 eviction 정책으로 섞지 않습니다.
- replica, persistence, backup, high availability가 해결하는 문제를 구분합니다.
- 설정 변경 후 `INFO`, 애플리케이션 테스트, 장애·복구 훈련으로 실제 동작을 확인합니다.

## 참고 기준

이 디렉터리는 2026-07-10 기준 Redis 공식 문서를 우선 참고합니다. 관리형 서비스는 지원 버전, 명령 제한, failover, 백업, TLS 정책이 제품별로 다르므로 제공자 문서를 함께 확인합니다.

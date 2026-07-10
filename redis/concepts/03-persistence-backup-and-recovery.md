# Redis persistence, 백업, 복구

## 학습 목표

- RDB, AOF, 동시 사용, persistence 없음의 차이를 구분합니다.
- 복제와 백업이 해결하는 문제가 다르다는 점을 설명합니다.
- RPO와 RTO를 기준으로 복구 절차를 설계하고 검증합니다.

## 먼저 구분할 것

| 기능 | 해결하려는 문제 | 해결하지 못하는 것 |
| --- | --- | --- |
| persistence | 프로세스나 서버 재시작 후 데이터 복원 | 잘못된 삭제의 시점 복구를 자동 보장하지 않음 |
| replication | 노드 장애 시 사용할 최신 복제본 유지 | 삭제·손상 명령도 복제되므로 백업이 아님 |
| backup | 과거 시점의 데이터를 별도 위치에 보관 | 자동 failover를 제공하지 않음 |
| high availability | 장애 감지와 서비스 복구 시간 단축 | 모든 쓰기의 무손실을 보장하지 않음 |

`FLUSHALL`, 애플리케이션 버그, 데이터 손상이 replica에 그대로 전달될 수 있으므로 replica 자체를 백업으로 취급하지 않습니다.

## 방식 비교

| 방식 | 예상 데이터 유실 범위 | 장점 | 비용과 주의점 |
| --- | --- | --- | --- |
| 없음 | 메모리 데이터 전체 | 순수 캐시에서 단순함 | 원본에서 전체 재생성 가능해야 함 |
| RDB | 마지막 snapshot 이후 | compact, 백업과 큰 데이터 재시작에 유리 | snapshot 간 쓰기 유실, `fork()` 비용 |
| AOF `everysec` | 일반적으로 최근 약 1초 | RDB보다 촘촘한 변경 기록 | 파일 크기, fsync와 rewrite 비용 |
| RDB + AOF | AOF 기준 복구 + RDB 백업 활용 | 복구 선택지와 백업 편의 | 메모리·디스크·운영 복잡도 증가 |

RPO는 허용 가능한 데이터 유실 시간이고 RTO는 서비스 복구에 허용되는 시간입니다. “AOF를 켠다”보다 “장애 시 몇 초를 잃을 수 있고 몇 분 안에 복구해야 하는가”를 먼저 정합니다.

## 설정 예시

다음 설정은 AOF와 RDB를 **동시에** 사용합니다.

```conf
appendonly yes
appendfsync everysec

save 900 1
save 300 10
save 60 10000

dir /data
```

두 방식을 함께 사용하면 재시작 시 더 완전한 AOF가 우선 사용됩니다. Redis 7 이상 AOF는 base 파일과 incremental 파일을 manifest로 관리하는 multi-part 구조이므로 AOF 파일 하나만 임의로 복사하지 않습니다.

## 상태 확인

```bash
redis-cli INFO persistence
redis-cli LASTSAVE
redis-cli CONFIG GET dir dbfilename appendonly appenddirname appendfsync
```

특히 다음 필드를 경보 대상으로 검토합니다.

- `rdb_last_bgsave_status`
- `rdb_last_save_time`
- `aof_enabled`
- `aof_last_write_status`
- `aof_rewrite_in_progress`
- `aof_last_bgrewrite_status`

설정 파일에서 persistence를 켰더라도 디스크 권한, 용량, `fork()` 실패로 실제 저장이 실패할 수 있습니다.

## 백업 원칙

1. 백업 파일을 Redis 데이터 디렉터리와 다른 장애 도메인에 보관합니다.
2. 암호화, 보존 기간, 접근 권한, 삭제 정책을 함께 정합니다.
3. 백업 시점의 Redis 버전과 설정을 기록합니다.
4. RDB는 완료 상태를 확인한 파일을 복사합니다.
5. AOF rewrite 중 파일 일부만 복사하지 말고 제품 또는 운영 환경이 제공하는 일관된 백업 절차를 사용합니다.
6. 백업 성공 알림보다 격리된 환경의 실제 복원 성공을 기준으로 검증합니다.

## 복구 훈련 절차

```text
1. 복구할 시점과 RPO를 결정한다.
2. 새 격리 환경에 같은 계열 Redis 버전을 준비한다.
3. Redis를 중지한 상태에서 검증된 RDB 또는 AOF 세트를 배치한다.
4. 파일 소유권과 redis.conf의 dir/dbfilename/appenddirname을 확인한다.
5. Redis를 시작하고 로그와 INFO persistence를 확인한다.
6. DBSIZE, 샘플 키, TTL, 애플리케이션 읽기 테스트로 데이터를 검증한다.
7. 복구 시간과 수동 단계를 기록해 다음 훈련을 개선한다.
```

운영 원본 디렉터리에서 처음 복구를 시도하지 않습니다. 잘못된 파일 배치나 버전 차이로 원본까지 훼손할 수 있습니다.

## 메모리와 지연 시간 비용

RDB 생성과 AOF rewrite는 자식 프로세스를 만들며 쓰기가 많은 동안 copy-on-write 메모리를 추가로 사용할 수 있습니다. 데이터셋이 큰 환경에서는 평상시 메모리만 보고 인스턴스 크기를 결정하지 않습니다.

```bash
redis-cli INFO memory
redis-cli INFO persistence
redis-cli LATENCY DOCTOR
```

백업이나 rewrite 중 지연 시간, 메모리, 디스크 처리량을 부하 테스트에서 함께 측정합니다.

## 확인 질문

1. replica가 있어도 과거 시점 백업이 필요한 이유는 무엇인가요?
2. 순수 캐시에서 persistence를 끄기 전에 무엇을 검증해야 하나요?
3. AOF `everysec`의 유실 가능성과 비용은 무엇인가요?
4. 백업 파일 생성 성공만으로 복구 가능성을 보장할 수 없는 이유는 무엇인가요?

## 참고한 공식 문서

- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [Diagnosing latency issues](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency/)

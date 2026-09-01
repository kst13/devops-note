# Kafka 클러스터 전체 정지와 복구

## 증상

브로커 3대가 모두 내려가 프로듀서는 `TimeoutException` 또는 `NotEnoughReplicasException`을, 컨슈머는 폴링 결과 없음 상태가 됩니다. 정전, 동시 하드웨어 장애, 인증서 동시 만료, 디스크 동시 만수처럼 3대를 한 번에 잡는 공통 원인에서 발생합니다.

## 먼저 알아둘 것: 무엇을 잃고 무엇을 잃지 않는가

- 이미 토픽에 커밋된 데이터는 잃지 않습니다. RF3 + `unclean.leader.election.enable=false` 구성이면 데이터는 3대 디스크에 남아 있고, 클러스터가 복구되면 그대로 돌아옵니다.
- 컨슈머의 읽던 위치(오프셋)도 `__consumer_offsets` 토픽에 저장되어 함께 복구됩니다.
- 위험한 것은 정지 시간 동안 프로듀서가 새로 만든 데이터입니다. 프로듀서 버퍼가 차면 애플리케이션에 예외가 던져지고, 애플리케이션이 이를 보관하지 않으면 유실됩니다. 애플리케이션 측 대비는 [사용 가이드 07 장애 대비](../usage-guide/07-failure-resilience.md)를 참고합니다.

## 확인 방법

```bash
# 각 노드에서 컨테이너 상태와 마지막 로그 확인
docker ps -a
docker compose logs --tail 100 kafka

# 프로세스가 떠 있다면 포트가 열렸는지 확인
ss -ltn | grep -E '9092|9093|9094'
```

로그에서 종료 원인을 먼저 분류합니다. 자주 보는 패턴은 다음과 같습니다.

| 로그 패턴 | 원인 |
| --- | --- |
| `No space left on device` | 디스크 만수 — retention 조치가 먼저 필요 |
| `certificate_expired`, `handshake_failure` | TLS 인증서 만료 — 인증서 교체가 먼저 필요 |
| `InconsistentClusterIdException` | meta.properties의 클러스터 ID 불일치 — 아래 "디스크를 잃은 노드" 참고 |
| 종료 로그 없이 컨테이너만 내려감 | 정전·OOM 등 비정상 종료 — 바로 재기동 절차로 |

## 해결 방법 (복구 절차)

원인(디스크·인증서)이 해소된 상태를 전제로 합니다.

1. 각 노드에서 기동합니다. 순서는 무관합니다.

```bash
docker compose up -d
docker compose logs -f kafka
```

2. KRaft 컨트롤러 쿼럼은 3대 중 과반(2대)이 모여야 열립니다. 1대만 켜면 클러스터는 열리지 않는 것이 정상입니다.
3. 비정상 종료였다면 각 브로커가 세그먼트 복구 검사를 수행합니다. 데이터량에 따라 수십 분 걸릴 수 있으며, 로그에 복구 진행이 보이는 동안은 고장이 아니라 정상 과정입니다. 기다립니다.
4. 복구 검증:

```bash
# 모든 파티션에 리더가 있는지, ISR이 3개로 회복됐는지
kafka-topics.sh --bootstrap-server localhost:9094 --describe --command-config client.properties

# 미복제 파티션이 0인지 (모니터링 지표 UnderReplicatedPartitions=0)
kafka-topics.sh --bootstrap-server localhost:9094 --describe --under-replicated-partitions --command-config client.properties
```

5. 컨슈머 그룹의 lag가 줄어드는지 확인합니다. 정지 시간 동안 밀린 데이터를 따라잡는 동안 lag가 크게 보이는 것은 정상입니다.

## 디스크를 잃은 노드가 있는 경우

디스크 교체 등으로 데이터 디렉터리가 사라진 노드는 `meta.properties`가 없어 기동이 거부되거나 클러스터 ID 불일치 오류가 납니다. 그 노드만 스토리지를 다시 포맷해 재조인하면, 나머지 두 대의 레플리카에서 복제로 데이터가 되채워집니다. 포맷 시 클러스터 ID는 반드시 기존 값(.env의 `KAFKA_CLUSTER_ID`)을 사용합니다. 절차 상세는 [07 설치 문서](../concepts/07-kraft-cluster-installation.md)의 스토리지 포맷 장을 참고합니다.

3대의 디스크를 전부 잃은 경우가 유일한 실제 데이터 유실 상황입니다. Kafka에는 내장 백업이 없으므로 이때의 답은 사전에 준비한 DR(MirrorMaker 2 복제 클러스터, 또는 원본 소스에서 이벤트 재생)뿐입니다.

## 재발 방지

- 3대를 동시에 잡는 공통 원인을 제거합니다: 같은 전원·같은 스위치에 3대를 두지 않기, OS 패치·재기동은 1대씩 롤링으로.
- 인증서 만료일과 디스크 사용률(70% 경고)에 알림을 겁니다 → [모니터링 문서](../concepts/14-monitoring-prometheus-grafana.md)
- 복구 소요 시간을 한 번 실제로 측정해 둡니다. 전체 정지 → 재기동 → 검증까지의 시간을 알아야 장애 공지와 대응 판단이 가능합니다.
- 애플리케이션 팀에 [장애 대비 가이드](../usage-guide/07-failure-resilience.md)를 공유해, 클러스터 정지가 데이터 유실 사고가 아닌 지연 사고가 되도록 합니다.

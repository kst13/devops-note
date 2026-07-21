# KRaft 등장 배경과 ZooKeeper 대비 장단점

[Kafka 기본 개념](01-kafka-basics.md)에서 KRaft와 ZooKeeper의 차이를 표로 짧게 봤습니다. 이 문서는 **왜 KRaft가 등장했고, ZooKeeper 대비 무엇이 낫고 무엇을 새로 신경 써야 하는지**를 정리합니다. 이 판단이 [실서버 3대 클러스터 설계 방식](03-cluster-design.md)에서 KRaft combined 모드를 고르는 근거가 됩니다.

## 배경: ZooKeeper 체제의 한계

Kafka는 처음부터 메타데이터(브로커 목록, 컨트롤러 선출, 토픽·파티션 정보, ACL, 쿼터 등)를 **외부 ZooKeeper 앙상블**에 저장했습니다. 초기에는 합리적인 선택이었지만, 클러스터가 커지면서 한계가 드러났습니다.

1. **두 시스템 운영 부담** — Kafka와 ZooKeeper를 각각 설치·튜닝·보안·모니터링·업그레이드해야 합니다. 장애 지점과 학습 비용이 두 배입니다.
2. **느린 컨트롤러 failover** — 컨트롤러가 바뀌면 ZooKeeper에서 **모든 메타데이터를 다시 로드**했습니다. 파티션이 많을수록(수십만) 이 시간이 파티션 수에 비례해 늘어, failover가 수 초에서 수십 초까지 걸렸습니다.
3. **간접적인 메타데이터 전파** — `ZooKeeper → 컨트롤러 → 브로커(RPC)` 경로입니다. ZooKeeper 상태와 컨트롤러 인메모리 캐시가 어긋나는 불일치·split-brain 엣지 케이스가 존재했습니다.
4. **확장 한계** — 클러스터가 감당하는 파티션 수의 상한이 사실상 ZooKeeper 기반 메타데이터 관리에 묶여 있었습니다.

이 의존을 없애자는 제안이 **KIP-500(2019)** 이고, 그 결과물이 KRaft(Kafka Raft)입니다.

## KRaft의 동작 방식

메타데이터를 외부에 두지 않고, Kafka 내부에서 이벤트 로그로 관리합니다.

```text
[ZooKeeper 모드]
  ZooKeeper 앙상블(별도) ── 컨트롤러 ──(RPC)── 브로커들
        메타데이터 저장            캐시

[KRaft 모드]
  컨트롤러 쿼럼(Raft) ── __cluster_metadata 로그 ──(fetch)── 브로커들
     active 컨트롤러가 로그에 append        브로커가 delta 적용
```

- 메타데이터는 내부 토픽 **`__cluster_metadata`** 에 이벤트 로그로 저장됩니다(이벤트 소싱).
- 이 로그는 **컨트롤러들이 구성한 Raft 쿼럼**이 합의·복제하고, 그중 하나가 active 컨트롤러(리더)입니다.
- 브로커는 이 로그를 **컨슈머처럼 fetch** 해 변경분(delta)만 따라 적용합니다. 단일 진실 소스(single source of truth)가 됩니다.

## 한눈에 보기

| 항목 | ZooKeeper 모드 | KRaft 모드 |
| --- | --- | --- |
| 메타데이터 저장소 | 외부 ZooKeeper | 내부 토픽 `__cluster_metadata` (Raft) |
| 운영 컴포넌트 | Kafka + ZooKeeper 2종 | Kafka 1종 |
| 컨트롤러 failover | ZK에서 전량 재로딩 → 느림 | 로그가 이미 있어 거의 즉시 |
| 메타데이터 전파 | ZK→컨트롤러→브로커(RPC), 간접 | 브로커가 로그 fetch, 직접·일관 |
| 확장성 | 파티션 수 제약 | 수백만 파티션까지 |
| 보안 모델 | Kafka·ZK 따로 | 하나로 통합 |
| 역할 배치 | ZK 3 + broker N | `process.roles`로 combined/dedicated |

## 장점 (KRaft 선택 시)

- **운영 단순화**: 관리 대상이 Kafka 하나. ZooKeeper 앙상블의 설치·튜닝·JVM·보안·모니터링·업그레이드가 통째로 사라집니다.
- **빠른 컨트롤러 failover**: 메타데이터가 이미 로그로 복제돼 있어 새 컨트롤러가 거의 즉시 인계합니다.
- **대규모 확장**: 파티션 수백만 규모까지. ZK 기반 관리의 상한을 제거합니다.
- **일관된 메타데이터**: 내부 로그가 단일 진실 소스라 캐시 불일치·split-brain 엣지 케이스가 줄어듭니다.
- **보안 모델 통합**: 인증·TLS를 Kafka 하나로 관리합니다.
- **자원 절약**: 소규모에서 ZooKeeper 3대를 따로 띄우지 않아도 됩니다. 3대 combined 구성이 가능한 이유입니다.
- **미래 호환**: 4.x부터 KRaft 전용이라 신규 기능·버그픽스가 KRaft에 집중됩니다.

## 주의할 점 (단점 / 새로 챙길 것)

- **상대적으로 짧은 검증 기간**: ZooKeeper는 10년 넘게 단련됐지만, KRaft의 프로덕션 판정은 2022년(3.3)입니다. 오래된 운영 노하우·사례는 ZK 쪽에 더 많습니다.
- **운영 친숙도**: 팀이 `zkCli`·ZK 스냅샷 진단에 익숙하다면, KRaft의 `kafka-metadata-quorum`·`__cluster_metadata` 로그 진단을 새로 익혀야 합니다.
- **마이그레이션 비용**: 기존 ZK 클러스터를 KRaft로 옮기는 것은 별도 절차이고 리스크가 있습니다. "신규 구축이면 처음부터 KRaft"가 정답인 이유입니다.
- **컨트롤러 쿼럼 설계 필요**: 쿼럼 정족수(과반)를 잃으면 메타데이터 쓰기가 멈춥니다. 컨트롤러 수와 배치(랙/AZ 분산)를 직접 챙겨야 합니다.
- **일부 도구 지연**: ZK 전제로 만들어진 오래된 모니터링·서드파티 도구가 KRaft 대응이 늦을 수 있습니다(최근 대부분 해소되는 중).
- **combined 모드의 자원 경합**: 소규모에선 편하지만 broker 부하와 controller 부하가 한 프로세스에 섞입니다. 대규모로 가면 dedicated 컨트롤러로 분리를 검토합니다.

## 버전 타임라인

```text
2.8 (2021.04)  KRaft 프리뷰(early access)
3.3 (2022.10)  신규 클러스터 프로덕션 준비 완료
3.4 ~ 3.6      ZooKeeper → KRaft 마이그레이션 경로 제공
3.5            ZooKeeper deprecated 선언
4.0 (2025)     ZooKeeper 지원 완전 제거 → KRaft 전용
```

## 결론 (실서버 3대 신규 구축 기준)

- 신규 구축이므로 **KRaft가 사실상 정답**입니다. 마이그레이션 리스크가 없고, ZooKeeper 3대를 아낄 수 있으며, 4.x는 KRaft 전용이라 선택지도 없습니다.
- 단점의 대부분은 "기존 ZooKeeper 자산이 있을 때"의 이야기라 신규 구축에는 거의 해당하지 않습니다.
- 다만 KRaft라고 공짜는 아니어서, **컨트롤러 쿼럼(3표, 과반 2) 배치와 `kafka-metadata-quorum`으로 상태를 확인하는 운영 습관**은 새로 들여야 합니다. 이 부분은 [실서버 3대 클러스터 설계 방식](03-cluster-design.md)과 [KRaft 3노드 클러스터 설치 및 설정 방법](04-kraft-cluster-installation.md)에 반영돼 있습니다.

한 줄 요약: **ZooKeeper 대비 운영·성능·확장에서 이득이 크고, 유일한 실질 단점은 역사가 짧아 운영 노하우를 새로 쌓아야 한다는 점** 입니다. 신규 구축에서는 트레이드오프가 명확히 KRaft 쪽으로 기웁니다.

## 참고한 공식 문서

- [KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
- [KRaft Overview](https://kafka.apache.org/documentation/#kraft)
- [Kafka 4.0 Release Notes (ZooKeeper 제거)](https://kafka.apache.org/documentation/#upgrade)

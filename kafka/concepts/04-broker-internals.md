# 브로커 내부 구조: 네트워크 스레드, 큐, I/O 스레드

브로커가 클라이언트 요청을 처리하는 내부 흐름을 정리합니다. `num.network.threads`, `num.io.threads` 등 성능 관련 설정이 실제로 어디에 작용하는지 이해하는 데 필요한 내용입니다.

## 요청 처리 흐름

```text
 Producer/Consumer             Broker 내부                              디스크
 ─────────────────      ──────────────────────────                   ──────────

              ①            ┌─────────────────┐
  request ────────────────► │ Network Thread   │
              TCP 수신      │ (num.network.    │
                            │  threads=3)      │
                            │                  │
                            │ 소켓에서 요청을    │
                            │ 읽어서 큐에 넣음   │
                            └────────┬─────────┘
                                     │
                                ② 요청 전달
                                     │
                                     ▼
                            ┌─────────────────┐
                            │  Request Queue   │
                            │                  │
                            │ [req1]           │
                            │ [req2]           │
                            │ [req3]           │
                            │ [...]            │
                            └────────┬─────────┘
                                     │
                                ③ 큐에서 꺼냄
                                     │
                                     ▼
                            ┌─────────────────┐       ④          ┌──────────┐
                            │  I/O Thread      │ ──────────────►  │          │
                            │ (num.io.         │    디스크 쓰기    │ log.dirs │
                            │  threads=8)      │                  │          │
                            │                  │ ◄──────────────  │          │
                            │ 실제 메시지 처리:  │    디스크 읽기    │          │
                            │ - 쓰기 (produce)  │                  │          │
                            │ - 읽기 (fetch)    │                  │          │
                            │ - 메타데이터 조회  │                  └──────────┘
                            └────────┬─────────┘
                                     │
                                ⑤ 처리 결과
                                     │
                                     ▼
                            ┌─────────────────┐
                            │ Response Queue   │
                            │                  │
                            │ [res1]           │
                            │ [res2]           │
                            └────────┬─────────┘
                                     │
                                ⑥ 응답 전달
                                     │
                                     ▼
                            ┌─────────────────┐
              ⑦             │ Network Thread   │
  response ◄─────────────── │                  │
              TCP 송신      │ 응답을 소켓으로    │
                            │ 돌려보냄          │
                            └─────────────────┘
```

| 단계 | 담당 | 하는 일 |
| --- | --- | --- |
| ① | Network Thread | 클라이언트 TCP 연결에서 요청을 읽음 |
| ② | | 요청을 Request Queue에 넣음 |
| ③ | I/O Thread | 큐에서 요청을 꺼냄 |
| ④ | I/O Thread | 디스크 읽기/쓰기 등 실제 처리 수행 |
| ⑤ | | 처리 결과를 Response Queue에 넣음 |
| ⑥⑦ | Network Thread | 응답을 클라이언트에게 전송 |

## 왜 스레드를 분리하는가

네트워크는 빠르고 디스크는 느립니다. 둘을 같은 스레드에서 처리하면 디스크 작업 때문에 네트워크 수신이 막힙니다.

```text
Network Thread (3개)              I/O Thread (8개)
┌───┐ ┌───┐ ┌───┐               ┌───┐ ┌───┐ ┌───┐ ┌───┐
│ N1│ │ N2│ │ N3│               │IO1│ │IO2│ │IO3│ │IO4│
└───┘ └───┘ └───┘               ├───┤ ├───┤ ├───┤ ├───┤
  빠름 (네트워크 I/O)              │IO5│ │IO6│ │IO7│ │IO8│
  소켓 읽기/쓰기만 담당             └───┘ └───┘ └───┘ └───┘
  병목 적음                         느림 (디스크 I/O)
                                   실제 데이터 처리 담당
                                   병목 가능성 높음
```

분리하면 네트워크 스레드는 요청을 계속 받아들이고, I/O 스레드는 자기 속도로 처리할 수 있습니다. I/O 스레드(8개)가 네트워크 스레드(3개)보다 많은 이유는 디스크 쪽이 더 오래 걸리기 때문입니다.

## 스레드풀 상세 (Acceptor·Purgatory·그 외 풀)

스레드풀 = **고정된 워커 스레드 + 공유 작업 큐**입니다. 요청마다 스레드를 새로 만들지 않고, 미리 만든 스레드가 큐에서 일을 꺼내 처리하고 재사용합니다. 동시 실행 수를 고정해 자원 폭주를 막고, 큐로 유입/처리 속도를 분리(backpressure)합니다.

```text
 새 TCP 연결 → [Acceptor 1개] 연결만 수락, 프로세서에 배분
              → [Network 풀 3개] 소켓 읽기 → Request Queue 넣기
              → [Request Queue] 유입 완충
              → [I/O 풀 8개] 큐에서 꺼내 실제 처리 → Response Queue
              → [Network 풀] 소켓으로 응답 전송
```

- **Acceptor**: 리스너당 1개. 새 연결만 수락해 네트워크 스레드에 넘깁니다.
- **Request Queue**: 요청이 처리보다 빨리 오면 여기 쌓입니다. **큐가 계속 차오르면** I/O 스레드가 못 따라간다는 신호(→ `num.io.threads` 증설 시점).
- **Purgatory(대기실)**: `acks=all` 쓰기나 `fetch.min.bytes` 대기처럼 **즉시 못 끝내는 요청**을 park합니다. I/O 스레드가 붙잡고 기다리지 않고 다음 일을 처리하다가, 조건이 충족되면(팔로워 복제 완료 등) 꺼내 완성합니다. 덕분에 스레드가 대기로 낭비되지 않습니다.

그 외 전용 풀:

| 풀 | 설정 | 역할 |
| --- | --- | --- |
| Replica fetcher | `num.replica.fetchers` | 팔로워가 리더에서 데이터를 가져와 복제 |
| Log cleaner | `log.cleaner.threads` | 로그 압축(compaction) |
| Background | `background.threads` | 세그먼트 삭제 등 백그라운드 |

## I/O 스레드가 실제로 하는 일 (produce·fetch·복제)

"실제 처리"를 요청 종류별로 뜯어보면 다음과 같습니다.

### Produce (쓰기)

```text
 1) 검증          권한·토픽/파티션·크기 확인
 2) 로그 append   리더 파티션 세그먼트 끝에 추가 (순차 쓰기)
 3) offset 부여 + 인덱스(offset/time) 갱신
 4) 응답          acks=0 → 응답 없음 / acks=1 → 리더 저장 후 즉시
                  acks=all → Purgatory에 park (팔로워 복제 대기)
```

**중요 — "디스크 쓰기"의 실제 의미**: append는 바로 fsync가 아니라 **OS page cache(메모리)에 씁니다.** 실제 flush는 OS가 나중에 몰아서 합니다. 매 쓰기마다 디스크를 안 기다려 빠르고, **내구성은 fsync가 아니라 복제(RF3)로** 확보합니다. append는 세그먼트 끝에만 붙는 **순차 쓰기**라 빠릅니다.

### Fetch (읽기 — 컨슈머·팔로워 공통)

팔로워 복제도 같은 경로입니다(팔로워 = 리더에서 당겨가는 특수 컨슈머).

```text
 1) offset 위치 찾기   offset index로 로그 내 위치 계산
 2) 읽기               최근 데이터 → page cache / 오래된 데이터 → 디스크
 3) 전송               zero-copy(sendfile)로 page cache → 소켓 직접 전송
 4) 데이터 부족        fetch.min.bytes 미달이면 Purgatory에서 대기
```

**zero-copy와 예외**: 보통 읽기는 데이터를 JVM 힙에 복사하지 않고 page cache에서 소켓으로 바로 보내 저렴합니다. 단 **TLS(SASL_SSL)를 쓰면 암호화 때문에 zero-copy가 불가**해 CPU를 더 씁니다.

### 복제 확인과 High Watermark(HW)

`acks=all` produce가 완성되는 과정입니다.

```text
 리더: produce append(offset 100) → Purgatory park
 팔로워: fetch로 offset 100까지 복제
 리더: ISR 전원이 100까지 따라옴 → High Watermark = 100 전진
       → Purgatory의 produce 완성 → 프로듀서에 성공 응답 ✓
```

- **High Watermark(HW)**: ISR 전원이 복제 완료한 지점. **컨슈머는 HW까지만 읽습니다**(committed 데이터만) → 리더 장애 시에도 일관성 유지.
- 즉 "복제 확인"이란 리더가 **팔로워 fetch로 복제 진척을 추적**해 ISR 전원이 따라오면 **HW를 전진**시키고, 그제서야 `acks=all` 응답을 돌려주는 것입니다.

### 핵심 최적화 (왜 빠른가)

| 기법 | 내용 |
| --- | --- |
| 순차 쓰기 | 세그먼트 끝에만 append → 디스크에서도 빠름 |
| page cache | fsync 매번 안 함, 내구성은 복제로. 읽기도 캐시 적중 |
| zero-copy | 읽기 시 힙 복사 없이 page cache→소켓 (단 TLS면 불가) |

## 관련 설정

| 프로퍼티 | 기본값 | 의미 |
| --- | --- | --- |
| `num.network.threads` | `3` | 네트워크 요청을 수신/응답하는 스레드 수 |
| `num.io.threads` | `8` | 요청을 실제로 처리(디스크 R/W)하는 스레드 수 |
| `socket.send.buffer.bytes` | `102400` (100KB) | TCP 소켓 송신 버퍼 (SO_SNDBUF) |
| `socket.receive.buffer.bytes` | `102400` (100KB) | TCP 소켓 수신 버퍼 (SO_RCVBUF) |
| `socket.request.max.bytes` | `104857600` (100MB) | 한 번의 요청에서 허용하는 최대 크기 |
| `num.recovery.threads.per.data.dir` | `1` | 브로커 시작 시 로그 세그먼트 복구·인덱스 재구축 스레드 수 |

## 튜닝 기준

처음에는 기본값으로 시작하고, 모니터링에서 병목이 보일 때 조정합니다.

- **Request Queue가 계속 쌓이면** → I/O 스레드(`num.io.threads`)를 늘립니다. 요청 처리 속도가 수신 속도를 못 따라가는 상황입니다.
- **네트워크 수신이 밀리면** → 네트워크 스레드(`num.network.threads`)를 늘립니다. 소켓 읽기/쓰기 자체가 병목인 상황입니다.
- **브로커 재시작이 오래 걸리면** → `num.recovery.threads.per.data.dir`을 늘려 로그 복구를 병렬화합니다. 단, 디스크 부하가 늘어납니다.

## 참고한 공식 문서

- [Broker Configs](https://kafka.apache.org/documentation/#brokerconfigs)

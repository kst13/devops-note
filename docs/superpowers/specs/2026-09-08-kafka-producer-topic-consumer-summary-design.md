# Kafka 프로듀서·토픽·컨슈머 핵심 정리 문서 설계

## 배경

`kafka/concepts/01~03`은 토픽·프로듀서·컨슈머를 각각 별도 문서로 자세히 다루고, `usage-guide/01~04`는 개발자 실무 규칙을 다룬다. 세 주제를 한 번에 복습하거나 면접을 준비할 때 한 문서로 훑을 수 있는 자료는 없다.

## 목표와 비목표

**목표**
- 프로듀서·토픽·컨슈머 개념을 한 문서에서 관계 중심으로 설명한다 (개념 설명 위주).
- 문서 끝에 면접 예상 질문 10개 안팎을 짧은 모범 답변과 함께 둔다.
- 기존 상세 문서로 링크해 중복 서술은 최소화한다.

**비목표**
- 기존 `concepts/01~03`, `usage-guide/*`의 내용 이동·재구성.
- Spring 코드 예제(usage-guide가 담당).
- 브로커 내부 구조, KRaft, 보안 등 세 주제 밖의 내용.

## 산출물

- 신규: `kafka/concepts/16-producer-topic-consumer-summary.md`
  - 제목: `프로듀서·토픽·컨슈머 핵심 정리 (면접 대비)`
  - 두 자리 접두사 16 → 난이도 `중급`, 개념 카테고리 마지막에 정렬.
- 수정: `kafka/README.md` 추천 학습 순서에 14·15와 함께 16 항목 추가 (14·15는 현재 목록에 빠져 있음).
- `web/app/data/content.generated.json`은 `npm run sync-content`로 재생성.

## 문서 구성

1. **한눈에 보기** — 프로듀서/토픽(파티션)/컨슈머 3자 관계 그림 + 표.
2. **토픽과 파티션** — 논리 채널 vs 물리 로그, 오프셋, 파티션 결정(key 해시·sticky), 파티션 수 제약, 보관(retention)·compaction, 복제·ISR·`min.insync.replicas`.
3. **프로듀서** — 전송 파이프라인(serializer → partitioner → accumulator → sender), `acks` 3종, 멱등 프로듀서(PID·시퀀스), 재시도와 순서, 배치·`linger.ms`·압축, 트랜잭션 프로듀서 개요.
4. **컨슈머** — pull 모델, 오프셋과 커밋(자동/수동, 커밋 시점과 유실·중복), 컨슈머 그룹과 1파티션:1컨슈머, 리밸런싱(eager vs cooperative, `max.poll.interval.ms`), 전달 보장 3종(at-most/at-least/exactly-once), lag.
5. **자주 헷갈리는 비교** — `acks` vs `min.insync.replicas`, 멱등 프로듀서 vs 멱등 컨슈머, 컨슈머 그룹 vs 큐, 파티션 수 vs 컨슈머 수.
6. **면접 예상 질문** — 10개 안팎, 각 답변 3~5줄.
7. **관련 문서** — 01·02·03·13·12, usage-guide 03·04.

## 작성 원칙

- 한국어 산문, ATX 헤딩, 언어 태그 있는 코드 블록(그림은 `text`).
- 웹 렌더러 지원 문법만 사용(중첩 목록 없음, `##`/`###`, 표, 인용, 인라인 코드·볼드·링크).
- 설정값은 "왜"를 함께 적는다.

## 검증

- `cd web && npm run sync-content` 후 생성 JSON에 새 문서 id가 있는지 확인.
- `npm run lint && npm test` 통과.
- `git diff --check` 통과.

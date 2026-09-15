package com.osstem.sample.orderpayment.analytics;

import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 3단계 조회 API. 발표자료 8장의 세 조회 + 반영 지연.
 *
 * 모든 SQL 은 아래 CTE 위에서 돈다.
 *   - event_id 중복 제거: 재시작·되감기로 같은 이벤트가 두 번 적재될 수 있다 (lakehouse/concepts/06 3장)
 *   - occurred_ts: JSON 의 ISO-8601 문자열을 timestamp 로 변환. epoch 초 문자열(초기 버전이 남긴 값)도 받는다
 *   - written_at: Iceberg 숨은 컬럼 "$file_modified_time" — 데이터 파일이 MinIO 에 기록된 시각. 반영 지연 측정에 쓴다
 * 뷰 대신 CTE 를 쓰는 이유는 카탈로그(REST fixture)의 뷰 지원에 의존하지 않기 위해서다.
 */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private static final String DEDUP_CTE = """
            WITH pe AS (
              SELECT event_id, event_type, order_id, payment_id, customer_id, amount, method, failure_reason, occurred_at,
                     coalesce(try(from_iso8601_timestamp(occurred_at)), from_unixtime(try_cast(occurred_at AS double))) AS occurred_ts,
                     "$file_modified_time" AS written_at,
                     row_number() OVER (PARTITION BY event_id ORDER BY "$file_modified_time") AS rn
              FROM payment_events
            ),
            payment_events_dedup AS (SELECT * FROM pe WHERE rn = 1),
            oe AS (
              SELECT event_id, event_type, order_id, customer_id, item_name, amount, occurred_at,
                     coalesce(try(from_iso8601_timestamp(occurred_at)), from_unixtime(try_cast(occurred_at AS double))) AS occurred_ts,
                     "$file_modified_time" AS written_at,
                     row_number() OVER (PARTITION BY event_id ORDER BY "$file_modified_time") AS rn
              FROM order_events
            ),
            order_events_dedup AS (SELECT * FROM oe WHERE rn = 1)
            """;

    private final AnalyticsRepository repo;

    public AnalyticsController(AnalyticsRepository repo) {
        this.repo = repo;
    }

    private static int clampDays(int days) {
        return Math.max(1, Math.min(days, 365));
    }

    /** 결제 성공률: 일별·결제수단별 시도/성공/실패. */
    @GetMapping("/payment-success-rate")
    public List<Map<String, Object>> successRate(@RequestParam(defaultValue = "7") int days) {
        return repo.query(DEDUP_CTE + """
                SELECT date(occurred_ts) AS day,
                       method,
                       count_if(event_type = 'PAYMENT_REQUESTED') AS requested,
                       count_if(event_type = 'PAYMENT_COMPLETED') AS completed,
                       count_if(event_type = 'PAYMENT_FAILED')    AS failed,
                       round(100.0 * count_if(event_type = 'PAYMENT_COMPLETED')
                             / nullif(count_if(event_type = 'PAYMENT_REQUESTED'), 0), 1) AS success_rate_pct
                FROM payment_events_dedup
                WHERE occurred_ts >= current_timestamp - interval '%d' day
                GROUP BY 1, 2
                ORDER BY 1 DESC, 2
                """.formatted(clampDays(days)));
    }

    /** 실패 사유별 건수. */
    @GetMapping("/payment-failures")
    public List<Map<String, Object>> failures(@RequestParam(defaultValue = "7") int days) {
        return repo.query(DEDUP_CTE + """
                SELECT failure_reason, method, count(*) AS cnt, sum(amount) AS amount
                FROM payment_events_dedup
                WHERE event_type = 'PAYMENT_FAILED'
                  AND occurred_ts >= current_timestamp - interval '%d' day
                GROUP BY 1, 2
                ORDER BY cnt DESC
                """.formatted(clampDays(days)));
    }

    /** 취소·환불 현황: 일별 주문 생성/취소 건수, 환불 금액. */
    @GetMapping("/cancellations")
    public List<Map<String, Object>> cancellations(@RequestParam(defaultValue = "7") int days) {
        return repo.query(DEDUP_CTE + """
                , o AS (
                  SELECT date(occurred_ts) AS day,
                         count_if(event_type = 'ORDER_CREATED')   AS created,
                         count_if(event_type = 'ORDER_CANCELLED') AS cancelled
                  FROM order_events_dedup
                  WHERE occurred_ts >= current_timestamp - interval '%1$d' day
                  GROUP BY 1
                ), r AS (
                  SELECT date(occurred_ts) AS day,
                         count(*) AS refunds, sum(amount) AS refund_amount
                  FROM payment_events_dedup
                  WHERE event_type = 'PAYMENT_REFUNDED'
                    AND occurred_ts >= current_timestamp - interval '%1$d' day
                  GROUP BY 1
                )
                SELECT o.day, o.created, o.cancelled,
                       coalesce(r.refunds, 0) AS refunds, coalesce(r.refund_amount, 0) AS refund_amount,
                       round(100.0 * o.cancelled / nullif(o.created, 0), 1) AS cancel_rate_pct
                FROM o LEFT JOIN r ON o.day = r.day
                ORDER BY o.day DESC
                """.formatted(clampDays(days)));
    }

    /**
     * 주문·결제 불일치 후보: 주문은 생성됐는데 일정 시간이 지나도 결제 이벤트(요청·완료·실패)가 하나도 없는 주문.
     * 반영 지연 때문에 방금 만든 주문이 잡히지 않도록 grace 초 이전 주문만 본다.
     */
    @GetMapping("/mismatches")
    public List<Map<String, Object>> mismatches(@RequestParam(defaultValue = "120") int graceSeconds) {
        return repo.query(DEDUP_CTE + """
                SELECT o.order_id, o.customer_id, o.item_name, o.amount, o.occurred_at AS ordered_at,
                       date_diff('second', o.occurred_ts, current_timestamp) AS age_seconds
                FROM order_events_dedup o
                LEFT JOIN payment_events_dedup p ON p.order_id = o.order_id
                WHERE o.event_type = 'ORDER_CREATED'
                  AND p.order_id IS NULL
                  AND o.occurred_ts < current_timestamp - interval '%d' second
                  AND NOT EXISTS (SELECT 1 FROM order_events_dedup c
                                  WHERE c.order_id = o.order_id AND c.event_type = 'ORDER_CANCELLED')
                ORDER BY o.occurred_ts DESC
                LIMIT 100
                """.formatted(Math.max(0, Math.min(graceSeconds, 86_400))));
    }

    /** 최근 결제 이벤트가 Lakehouse 파일로 기록되기까지 걸린 시간 (앱 발행 시각 → 데이터 파일 기록 시각). */
    @GetMapping("/freshness")
    public List<Map<String, Object>> freshness(@RequestParam(defaultValue = "20") int limit) {
        return repo.query(DEDUP_CTE + """
                SELECT event_id, event_type, order_id, payment_id, method, amount, occurred_at,
                       written_at AS committed_at,
                       date_diff('second', occurred_ts, written_at) AS lag_seconds
                FROM payment_events_dedup
                ORDER BY occurred_ts DESC
                LIMIT %d
                """.formatted(Math.max(1, Math.min(limit, 200))));
    }

    /** 특정 주문의 Lakehouse 이력 (H2 상태와 나란히 보여 주기 위해). */
    @GetMapping("/order-timeline")
    public List<Map<String, Object>> timeline(@RequestParam String orderId) {
        return repo.query(DEDUP_CTE + """
                SELECT event_type, occurred_at, CAST(NULL AS varchar) AS payment_id, CAST(NULL AS varchar) AS method,
                       amount, CAST(NULL AS varchar) AS failure_reason
                FROM order_events_dedup WHERE order_id = ?
                UNION ALL
                SELECT event_type, occurred_at, payment_id, method, amount, failure_reason
                FROM payment_events_dedup WHERE order_id = ?
                ORDER BY occurred_at
                """, orderId, orderId);
    }
}

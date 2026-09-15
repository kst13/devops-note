package com.osstem.sample.orderpayment.payment;

import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * 실제 PG 대신 규칙으로 성공·실패를 정하는 가짜 결제. 3단계 조회에 실패 데이터가 있어야
 * 결제 성공률·실패 사유 집계가 의미를 가진다.
 *
 * <ul>
 *   <li>금액 1,000,000 이상 → LIMIT_EXCEEDED</li>
 *   <li>POINT 이고 50,000 초과 → INSUFFICIENT_POINT</li>
 * </ul>
 */
@Component
public class FakePaymentGateway {

    public static final long CARD_LIMIT = 1_000_000L;
    public static final long POINT_LIMIT = 50_000L;

    /** 실패 사유를 돌려준다. 비어 있으면 성공. */
    public Optional<String> tryCharge(long amount, Payment.Method method) {
        if (amount >= CARD_LIMIT) {
            return Optional.of("LIMIT_EXCEEDED");
        }
        if (method == Payment.Method.POINT && amount > POINT_LIMIT) {
            return Optional.of("INSUFFICIENT_POINT");
        }
        return Optional.empty();
    }
}

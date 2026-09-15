package com.osstem.sample.orderpayment.payment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FakePaymentGatewayTest {

    private final FakePaymentGateway gateway = new FakePaymentGateway();

    @Test
    void cardUnderLimitSucceeds() {
        assertThat(gateway.tryCharge(999_999, Payment.Method.CARD)).isEmpty();
    }

    @Test
    void amountAtLimitFails() {
        assertThat(gateway.tryCharge(1_000_000, Payment.Method.CARD)).contains("LIMIT_EXCEEDED");
    }

    @Test
    void pointOverLimitFails() {
        assertThat(gateway.tryCharge(50_001, Payment.Method.POINT)).contains("INSUFFICIENT_POINT");
        assertThat(gateway.tryCharge(50_000, Payment.Method.POINT)).isEmpty();
    }
}

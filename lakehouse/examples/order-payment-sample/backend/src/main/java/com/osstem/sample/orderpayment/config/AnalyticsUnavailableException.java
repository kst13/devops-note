package com.osstem.sample.orderpayment.config;

/** Trino 조회가 꺼져 있거나 접속이 안 될 때. 주문·결제 기능과는 무관하므로 503 으로 분리한다. */
public class AnalyticsUnavailableException extends RuntimeException {

    public AnalyticsUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public AnalyticsUnavailableException(String message) {
        super(message);
    }
}

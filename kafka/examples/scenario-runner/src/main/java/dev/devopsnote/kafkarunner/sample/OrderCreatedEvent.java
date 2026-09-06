package dev.devopsnote.kafkarunner.sample;

/** usage-guide 03·04 의 OrderCreatedEvent — JSON 단계용. createdAt 은 ISO-8601 문자열(Jackson 시간 모듈 의존 없음). */
public record OrderCreatedEvent(String orderId, String customerId, long amount, String createdAt) {}

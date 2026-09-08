package dev.devopsnote.kafkarunner.sample;

/** usage-guide 03·04 의 OrderCreatedEvent — JSON 단계용. customerId 는 usage-guide 01 예시처럼 숫자, createdAt 은 ISO-8601 문자열(Jackson 시간 모듈 의존 없음). */
public record OrderCreatedEvent(String orderId, long customerId, long amount, String createdAt) {}

package dev.devopsnote.kafkarunner.command;

/** 명령 인자 형식 오류. RunnerApplication 이 종료 코드 2 로 바꾼다.
 *  IllegalArgumentException 의 하위 타입이라 일반적인 인자 검증 코드와도 호환된다. */
public class UsageException extends IllegalArgumentException {
    public UsageException(String message) { super(message); }
}

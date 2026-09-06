package dev.devopsnote.kafkarunner.command;

import java.util.List;

/** CLI 첫 인자로 선택되는 실행 단위. 반환값은 프로세스 종료 코드 (0 성공/PASS, 1 실패/FAIL).
 *  인자 형식 오류는 IllegalArgumentException 을 던지면 RunnerApplication 이 종료 코드 2 로 바꾼다. */
public interface Command {
    String name();
    String description();
    int run(List<String> args) throws Exception;
}

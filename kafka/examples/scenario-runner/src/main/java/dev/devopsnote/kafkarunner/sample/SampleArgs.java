package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.command.UsageException;
import java.util.List;

/** sample-* 명령의 선택 인자 [count] 파싱. 잘못된 값은 UsageException → 종료 코드 2. */
final class SampleArgs {
    private SampleArgs() {}

    static int count(List<String> args, int defaultValue) {
        if (args.size() > 1) throw new UsageException("인자는 [count] 하나뿐입니다: " + args);
        if (args.isEmpty()) return defaultValue;
        int count;
        try { count = Integer.parseInt(args.get(0)); }
        catch (NumberFormatException e) { throw new UsageException("count 는 정수여야 합니다: " + args.get(0)); }
        if (count <= 0) throw new UsageException("count 는 1 이상이어야 합니다: " + count);
        return count;
    }
}

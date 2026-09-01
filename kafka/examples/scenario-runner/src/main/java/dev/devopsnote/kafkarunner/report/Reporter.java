package dev.devopsnote.kafkarunner.report;

import dev.devopsnote.kafkarunner.ledger.JudgeResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** 콘솔 요약 + Markdown 리포트 저장. */
public class Reporter {
    public Path write(String reportDir, String scenario, String description, JudgeResult result) throws Exception {
        String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Path dir = Path.of(reportDir);
        Files.createDirectories(dir);
        Path file = dir.resolve(scenario + "-" + stamp + ".md");
        String verdict = result.pass() ? "PASS" : "FAIL";
        StringBuilder md = new StringBuilder();
        md.append("# 시나리오 리포트: ").append(scenario).append("\n\n")
          .append("- 판정: **").append(verdict).append("**\n")
          .append("- 설명: ").append(description).append("\n")
          .append("- 전송 성공: ").append(result.sentOk())
          .append(" / 미전송 잔여: ").append(result.sentFail())
          .append(" / 수신: ").append(result.received())
          .append(" / 유실: ").append(result.lost())
          .append(" / 중복: ").append(result.duplicates()).append("\n");
        if (!result.reasons().isEmpty()) {
            md.append("\n## FAIL 사유\n\n");
            result.reasons().forEach(reason -> md.append("- ").append(reason).append("\n"));
        }
        Files.writeString(file, md.toString());
        System.out.println("[" + verdict + "] " + scenario + " — 리포트: " + file);
        result.reasons().forEach(reason -> System.out.println("  - " + reason));
        return file;
    }
}

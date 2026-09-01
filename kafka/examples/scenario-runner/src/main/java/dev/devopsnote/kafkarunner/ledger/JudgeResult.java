package dev.devopsnote.kafkarunner.ledger;

import java.util.List;

public record JudgeResult(boolean pass, List<String> reasons,
                          long sentOk, long sentFail, long received, long lost, long duplicates) {}

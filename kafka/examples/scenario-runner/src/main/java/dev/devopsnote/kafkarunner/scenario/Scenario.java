package dev.devopsnote.kafkarunner.scenario;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.ledger.Expectation;
import dev.devopsnote.kafkarunner.ledger.Ledger;

public interface Scenario {
    String name();
    String description();
    Expectation expectation();
    /** 장애 주입·부하·대기를 수행하고 원장을 채운다. */
    void run(RunnerProperties props, FaultInjector injector, Ledger ledger) throws Exception;
}

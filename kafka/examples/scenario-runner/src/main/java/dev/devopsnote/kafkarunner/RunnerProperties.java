package dev.devopsnote.kafkarunner;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "runner")
public record RunnerProperties(String bootstrapServers, String topic, int partitions,
                               List<String> containers, String reportDir) {}

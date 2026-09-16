package com.osstem.sample.orderpayment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** application.yml 의 app.* 블록. */
@ConfigurationProperties(prefix = "app")
public record AppProperties(Topics topics, String sourceSystem, Analytics analytics, Pipeline pipeline) {

    public record Topics(String order, String payment) {}

    public record Analytics(boolean enabled, String trinoUrl, String trinoUser) {}

    /** 파이프라인 화면: Kafka Connect REST 주소와 상태를 볼 커넥터 이름 (컨슈머 그룹은 connect-<이름>) */
    public record Pipeline(String connectUrl, java.util.List<String> connectors) {}
}

package com.osstem.sample.orderpayment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** application.yml 의 app.* 블록. */
@ConfigurationProperties(prefix = "app")
public record AppProperties(Topics topics, String sourceSystem, Analytics analytics) {

    public record Topics(String order, String payment) {}

    public record Analytics(boolean enabled, String trinoUrl, String trinoUser) {}
}

package com.osstem.commerce.order;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * 로컬 검증 전용. 운영에서는 토픽을 앱이 만들지 않고 플랫폼팀에 요청한다(usage-guide 02).
 * compose 가 auto.create.topics.enable=false 라 이 빈이 없으면 첫 전송이 UNKNOWN_TOPIC 으로 실패한다.
 */
@Configuration
@Profile("!prod")
class LocalTopicConfig {

    @Bean
    NewTopic orderCreatedTopic(@Value("${app.topic}") String topic) {
        return new NewTopic(topic, 3, (short) 1);
    }
}

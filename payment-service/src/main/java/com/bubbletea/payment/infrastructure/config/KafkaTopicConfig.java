package com.bubbletea.payment.infrastructure.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * confirm/cancel 요청 토픽은 파티션 수가 @KafkaListener의 concurrency와 맞아야
 * 실제로 병렬 처리된다 - 브로커 기본값(파티션 1개)으로 자동 생성되면 concurrency를
 * 아무리 올려도 컨슈머 스레드 1개만 일하게 된다. 그래서 명시적으로 선언한다.
 */
@Configuration
public class KafkaTopicConfig {

    private static final int EXECUTION_PARTITIONS = 20;

    @Bean
    public NewTopic confirmRequestedTopic() {
        return TopicBuilder.name("payment.internal.confirm-requested")
                .partitions(EXECUTION_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cancelRequestedTopic() {
        return TopicBuilder.name("payment.internal.cancel-requested")
                .partitions(EXECUTION_PARTITIONS)
                .replicas(1)
                .build();
    }
}

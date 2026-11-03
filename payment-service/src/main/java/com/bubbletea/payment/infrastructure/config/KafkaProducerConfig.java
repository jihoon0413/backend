package com.bubbletea.payment.infrastructure.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.bootstrap-servers:bubbletea-kafka:9094}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Outbox 배치 발행을 건별 블로킹(.get()) 대신 병렬로 돌리기 위한 콜백 전용 풀.
     * Kafka 클라이언트의 네트워크 I/O 스레드에서 직접 DB 갱신을 하지 않도록 분리한다.
     */
    @Bean
    public Executor outboxPublishExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        executor.setThreadNamePrefix("outbox-publish-");
        executor.initialize();
        return executor;
    }

    /**
     * confirm/cancel 접수 이벤트 발행용 - 결과통지(outboxPublishExecutor)와 풀을 분리해서
     * 한쪽이 몰려도 다른 쪽 콜백 처리가 밀리지 않게 한다.
     */
    @Bean
    public Executor requestOutboxPublishExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        executor.setThreadNamePrefix("request-outbox-publish-");
        executor.initialize();
        return executor;
    }

}

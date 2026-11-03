package com.bubbletea.payment.processor;

import com.bubbletea.payment.entity.PaymentOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.repository.PaymentOutboxRepository;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventProcessor {
    private final PaymentOutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    // Spring이 이름(outboxPublishExecutor)으로 매칭해서 주입 - Executor 타입 빈이 Spring Boot 기본
    // applicationTaskExecutor까지 2개라 타입만으론 모호해지는데, 필드명=빈 이름이라 자동으로 구분된다.
    private final Executor outboxPublishExecutor;

    private static final String HEADER_DOMAIN = "X-Domain";
    private static final String HEADER_EVENT_TYPE = "X-Event-Type";
    private static final String HEADER_EVENT_TIMESTAMP = "X-Event-Timestamp";

    /**
     * 건별로 Kafka 전송 응답을 블로킹(.get())하며 기다리던 걸 제거하고, 배치 안의 모든 건을
     * 동시에 보낸 뒤 각자 콜백에서 상태를 갱신한다. 이전엔 50건을 순차 전송하면 건당 지연이
     * 그대로 누적돼서(배치가 클수록 뒤쪽 건은 몇 초씩 밀림) 처리량이 "배치 크기 / (건당 전송시간 합)"로
     * 제한됐는데, 병렬로 쏘면 처리량이 "배치 크기 / (가장 느린 건의 전송시간)"에 가까워진다.
     */
    public CompletableFuture<Void> sendToKafkaAndUpdateStatusAsync(PaymentOutbox outbox) {
        Object eventDto;
        try {
            // 결과 통지(PaymentResultEvent)뿐 아니라 confirm/cancel 요청 이벤트 등 다른 페이로드 타입도
            // 같은 릴레이로 실어날라야 해서 특정 타입으로 고정하지 않고 범용으로 역직렬화한다.
            // USE_BIG_DECIMAL_FOR_FLOATS를 이 호출에만 적용 - 안 하면 금액이 Double로 떨어져 정밀도가 깨진다.
            eventDto = objectMapper.reader()
                    .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .readValue(outbox.getPayload(), Object.class);
        } catch (Exception e) {
            log.error("[Outbox Scheduler] 페이로드 역직렬화 실패 (아웃박스 ID: {}). PENDING으로 되돌립니다. 원인: {}",
                    outbox.getId(), e.getMessage());
            markPending(outbox);
            return CompletableFuture.completedFuture(null);
        }

        String currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        Message<Object> message = MessageBuilder
                .withPayload(eventDto)
                .setHeader(KafkaHeaders.TOPIC, outbox.getTopic())
                .setHeader(KafkaHeaders.KEY, outbox.getMessageKey())
                .setHeader(HEADER_DOMAIN, outbox.getAggregateType())
                .setHeader(HEADER_EVENT_TYPE, outbox.getEventType())
                .setHeader(HEADER_EVENT_TIMESTAMP, currentTimestamp)
                .build();

        return kafkaTemplate.send(message)
                .thenRunAsync(() -> {
                    markProcessed(outbox);
                    log.info("[Outbox Scheduler] 카프카 발행 및 DB 업데이트 완료. ID: {}, Topic: {}", outbox.getId(), outbox.getTopic());
                }, outboxPublishExecutor)
                .exceptionally(ex -> {
                    log.error("[Outbox Scheduler] 카프카 발행 실패 (아웃박스 ID: {}). 다음 주기에 재시도합니다. 원인: {}",
                            outbox.getId(), ex.getMessage());
                    markPending(outbox);
                    return null;
                });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessed(PaymentOutbox outbox) {
        outbox.changeStatus(OutboxStatus.PROCESSED);
        outboxRepository.save(outbox);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPending(PaymentOutbox outbox) {
        outbox.changeStatus(OutboxStatus.PENDING);
        outboxRepository.save(outbox);
    }
}

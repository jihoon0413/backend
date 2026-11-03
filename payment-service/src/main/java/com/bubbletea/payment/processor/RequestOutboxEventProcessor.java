package com.bubbletea.payment.processor;

import com.bubbletea.payment.entity.PaymentRequestOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.repository.PaymentRequestOutboxRepository;
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

/**
 * confirm/cancel 접수(payment_request_outbox) 전용 발행기 - OutboxEventProcessor(결제 결과 통지)와
 * 로직은 같은 모양이지만 테이블/스레드풀을 의도적으로 분리해서 둔다. 하나로 합치면 "요청 접수"와
 * "결과 통지"가 다시 같은 relay를 경쟁하게 돼서 분리한 의미가 없어진다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RequestOutboxEventProcessor {
    private final PaymentRequestOutboxRepository requestOutboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Executor requestOutboxPublishExecutor;

    private static final String HEADER_DOMAIN = "X-Domain";
    private static final String HEADER_EVENT_TYPE = "X-Event-Type";
    private static final String HEADER_EVENT_TIMESTAMP = "X-Event-Timestamp";

    public CompletableFuture<Void> sendToKafkaAndUpdateStatusAsync(PaymentRequestOutbox outbox) {
        Object eventDto;
        try {
            eventDto = objectMapper.reader()
                    .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .readValue(outbox.getPayload(), Object.class);
        } catch (Exception e) {
            log.error("[Request Outbox Scheduler] 페이로드 역직렬화 실패 (아웃박스 ID: {}). PENDING으로 되돌립니다. 원인: {}",
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
                    log.info("[Request Outbox Scheduler] 접수 이벤트 발행 완료. ID: {}, Topic: {}", outbox.getId(), outbox.getTopic());
                }, requestOutboxPublishExecutor)
                .exceptionally(ex -> {
                    log.error("[Request Outbox Scheduler] 접수 이벤트 발행 실패 (아웃박스 ID: {}). 다음 주기에 재시도합니다. 원인: {}",
                            outbox.getId(), ex.getMessage());
                    markPending(outbox);
                    return null;
                });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessed(PaymentRequestOutbox outbox) {
        outbox.changeStatus(OutboxStatus.PROCESSED);
        requestOutboxRepository.save(outbox);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPending(PaymentRequestOutbox outbox) {
        outbox.changeStatus(OutboxStatus.PENDING);
        requestOutboxRepository.save(outbox);
    }
}

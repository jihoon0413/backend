package com.bubbletea.payment.processor;

import com.bubbletea.payment.entity.PaymentRequestOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.repository.PaymentRequestOutboxRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestOutboxProcessor {

    private final PaymentRequestOutboxRepository requestOutboxRepository;
    private final RequestOutboxEventProcessor requestOutboxEventProcessor;

    private final TransactionTemplate transactionTemplate;

    private final String processorId = UUID.randomUUID().toString();

    public void postEvent() {
        List<PaymentRequestOutbox> processingEvents = transactionTemplate.execute(status -> {
            int claimedCount = requestOutboxRepository.claimPendingEvents(processorId, LocalDateTime.now());
            if (claimedCount == 0) {
                return List.of();
            }
            return requestOutboxRepository.findByStatusAndProcessorId(OutboxStatus.PROCESSING, processorId);
        });

        if (processingEvents == null || processingEvents.isEmpty()) {
            return;
        }

        log.info("[Request Outbox Scheduler] 미발행 접수 이벤트 {}건 발견. 병렬 발행을 시작합니다.", processingEvents.size());

        List<CompletableFuture<Void>> futures = processingEvents.stream()
                .map(requestOutboxEventProcessor::sendToKafkaAndUpdateStatusAsync)
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public void cleanEvent() {
        int cleanedCount = requestOutboxRepository.cleanupStaleProcessingEvents(LocalDateTime.now(), LocalDateTime.now().minusMinutes(5));

        if (cleanedCount > 0) {
            log.warn("[Request Outbox Cleanup] 서버 장애로 고립되었던 좀비 레코드 {}건을 PENDING으로 안전하게 복구했습니다.", cleanedCount);
        }
    }
}

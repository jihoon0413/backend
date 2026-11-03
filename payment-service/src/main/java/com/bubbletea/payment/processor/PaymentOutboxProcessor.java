package com.bubbletea.payment.processor;

import com.bubbletea.payment.entity.PaymentOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.repository.PaymentOutboxRepository;
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
public class PaymentOutboxProcessor {

    private final PaymentOutboxRepository outboxRepository;
    private final OutboxEventProcessor outboxEventProcessor;

    private final TransactionTemplate transactionTemplate;

    private final String processorId = UUID.randomUUID().toString();

    public void postEvent() {
        List<PaymentOutbox> processingEvents = transactionTemplate.execute(status -> {
            int claimedCount = outboxRepository.claimPendingEvents(processorId, LocalDateTime.now());
            if (claimedCount == 0) {
                return List.of();
            }
            return outboxRepository.findByStatusAndProcessorId(OutboxStatus.PROCESSING, processorId);
        });

        if (processingEvents == null || processingEvents.isEmpty()) {
            return;
        }

        log.info("[Outbox Scheduler] 미발행 이벤트 {}건 발견. 병렬 발행을 시작합니다.", processingEvents.size());

        // 건별 블로킹(.get()) 순차 발행 대신, 배치 전체를 동시에 보내고 다 끝날 때까지만 기다린다.
        // 실패한 건은 각자 콜백에서 개별적으로 PENDING으로 되돌아가므로(sendToKafkaAndUpdateStatusAsync 내부),
        // 한 건이 실패해도 나머지 배치를 막지 않는다.
        List<CompletableFuture<Void>> futures = processingEvents.stream()
                .map(outboxEventProcessor::sendToKafkaAndUpdateStatusAsync)
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public void cleanEvent() {
        int cleanedCount = outboxRepository.cleanupStaleProcessingEvents(LocalDateTime.now(), LocalDateTime.now().minusMinutes(5));

        if (cleanedCount > 0) {
            log.warn("[Outbox Cleanup] 서버 장애로 고립되었던 좀비 레코드 {}건을 PENDING으로 안전하게 복구했습니다.", cleanedCount);
        }
    }
}

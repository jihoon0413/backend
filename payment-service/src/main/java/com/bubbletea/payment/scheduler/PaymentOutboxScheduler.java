package com.bubbletea.payment.scheduler;

import com.bubbletea.payment.processor.PaymentOutboxProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentOutboxScheduler {

    private final PaymentOutboxProcessor paymentOutboxProcessor;

    // confirm/cancel 접수 이벤트까지 같은 relay를 타면서 건당 처리량(outbox 3건/요청)이 늘어
    // 500ms/50건으로는 부하 상황에서 뒤로 밀리는 걸 확인함 - 주기를 줄여서 처리량을 늘림.
    @Scheduled(fixedDelay = 100)
    public void processOutboxEvents() {
        paymentOutboxProcessor.postEvent();
    }

    @Scheduled(fixedDelay = 300000)
    public void cleanupStaleOutboxEvents() {
        paymentOutboxProcessor.cleanEvent();
    }
}

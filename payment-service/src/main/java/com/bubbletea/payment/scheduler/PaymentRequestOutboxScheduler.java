package com.bubbletea.payment.scheduler;

import com.bubbletea.payment.processor.PaymentRequestOutboxProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * confirm/cancel 접수 이벤트 전용 스케줄러 - PaymentOutboxScheduler(결제 결과 통지)와 완전히 독립된
 * 틱으로 돈다. 같은 스케줄러를 공유하지 않아서 한쪽 물량이 늘어도 다른 쪽 주기에 영향을 주지 않는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestOutboxScheduler {

    private final PaymentRequestOutboxProcessor paymentRequestOutboxProcessor;

    @Scheduled(fixedDelay = 100)
    public void processRequestOutboxEvents() {
        paymentRequestOutboxProcessor.postEvent();
    }

    @Scheduled(fixedDelay = 300000)
    public void cleanupStaleRequestOutboxEvents() {
        paymentRequestOutboxProcessor.cleanEvent();
    }
}

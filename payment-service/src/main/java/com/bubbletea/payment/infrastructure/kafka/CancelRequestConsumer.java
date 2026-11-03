package com.bubbletea.payment.infrastructure.kafka;

import com.bubbletea.common.exception.AppException;
import com.bubbletea.payment.facade.PaymentCancelFacade;
import com.bubbletea.payment.global.exception.PaymentErrorCode;
import com.bubbletea.payment.infrastructure.kafka.dto.CancelRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CancelRequestConsumer {

    private final PaymentCancelFacade paymentCancelFacade;

    @KafkaListener(
            topics = "payment.internal.cancel-requested",
            groupId = "cancel-execution-group",
            concurrency = "10",
            containerFactory = "cancelRequestKafkaListenerContainerFactory"
    )
    public void consumeCancelRequest(
            ConsumerRecord<String, CancelRequestedEvent> record,
            Acknowledgment ack
    ) {
        try {
            CancelRequestedEvent event = record.value();
            paymentCancelFacade.cancel(event.userId(), event.request(), false);
            ack.acknowledge();
        } catch (AppException e) {
            if (e.getErrorCode() == PaymentErrorCode.EXTERNAL_SERVER_ERROR) {
                log.warn("토스 외부 서버 타임아웃 발생 (상태 유지) - 카프카 자체 재시도를 위해 예외 전달. PaymentId: {}", record.key());
                throw e;
            }
            log.error("결제 취소 처리 실패 (ErrorCode: {}, PaymentId: {})", e.getErrorCode(), record.key());
            ack.acknowledge();
        }
    }
}

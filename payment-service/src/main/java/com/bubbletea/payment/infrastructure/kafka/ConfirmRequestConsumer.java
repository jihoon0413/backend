package com.bubbletea.payment.infrastructure.kafka;

import com.bubbletea.common.exception.AppException;
import com.bubbletea.payment.facade.PaymentConfirmFacade;
import com.bubbletea.payment.global.exception.PaymentErrorCode;
import com.bubbletea.payment.service.dto.PaymentConfirmRequestDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConfirmRequestConsumer {

    private final PaymentConfirmFacade paymentConfirmFacade;

    @KafkaListener(
            topics = "payment.internal.confirm-requested",
            groupId = "confirm-execution-group",
            concurrency = "20",
            containerFactory = "confirmRequestKafkaListenerContainerFactory"
    )
    public void consumeConfirmRequest(
            ConsumerRecord<String, PaymentConfirmRequestDto> record,
            Acknowledgment ack
    ) {
        try {
            paymentConfirmFacade.confirm(record.value());
            ack.acknowledge();
        } catch (AppException e) {
            if (e.getErrorCode() == PaymentErrorCode.EXTERNAL_SERVER_ERROR) {
                log.warn("토스 외부 서버 타임아웃 발생 (상태 유지) - 카프카 자체 재시도를 위해 예외 전달. OrderId: {}", record.key());
                throw e;
            }
            log.error("결제 승인 처리 실패 (ErrorCode: {}, OrderId: {})", e.getErrorCode(), record.key());
            ack.acknowledge();
        }
    }
}

package com.bubbletea.payment.facade;

import com.bubbletea.payment.entity.PaymentRequestOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.global.exception.PaymentErrorCode;
import com.bubbletea.payment.global.exception.PaymentSystemException;
import com.bubbletea.payment.global.exception.PaymentTossApiException;
import com.bubbletea.payment.infrastructure.kafka.dto.CancelRequestedEvent;
import com.bubbletea.payment.repository.PaymentRequestOutboxRepository;
import com.bubbletea.payment.service.PaymentCancelService;
import com.bubbletea.payment.service.dto.PaymentCancelRequestDto;
import com.bubbletea.payment.service.dto.PaymentCancelResponseDto;
import com.bubbletea.payment.service.dto.data.PaymentCancelData;
import com.bubbletea.payment.service.external.TossBrandpayApiClient;
import com.bubbletea.payment.processor.PaymentPostProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCancelFacade {

    private static final String CANCEL_REQUESTED_TOPIC = "payment.internal.cancel-requested";

    private final TossBrandpayApiClient tossBrandpayApiClient;
    private final PaymentCancelService paymentCancelService;
    private final PaymentPostProcessor paymentPostProcessor;
    private final PaymentRequestOutboxRepository paymentRequestOutboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * HTTP 요청 스레드에서는 DB 작업만 하고 즉시 반환한다 (Toss 호출은 안 함).
     * readyCancel()이 이미 PaymentCancel을 REQUEST 상태로 만들어주므로 그게 곧 in-flight 마커가 되고,
     * 같은 트랜잭션에 outbox 적재까지 묶어서 전용 릴레이(PaymentRequestOutboxScheduler)가 Kafka로 중계하게 한다.
     */
    @Transactional
    public PaymentCancelData acceptCancel(Long userId, PaymentCancelRequestDto dto) {
        PaymentCancelData data = paymentCancelService.readyCancel(userId, dto, false);

        if (data == null) {
            // 이미 SUCCESS로 처리 완료된 멱등 요청 - 새로 접수할 게 없다.
            return null;
        }

        PaymentRequestOutbox outbox = PaymentRequestOutbox.builder()
                .aggregateType("payment")
                .aggregateId(data.paymentId())
                .topic(CANCEL_REQUESTED_TOPIC)
                .eventType("cancelRequested")
                .messageKey(dto.paymentId().toString())
                .payload(convertToJson(new CancelRequestedEvent(userId, dto)))
                .status(OutboxStatus.PENDING)
                .build();

        paymentRequestOutboxRepository.save(outbox);

        return data;
    }

    private String convertToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("JSON 변환 실패", e);
        }
    }

    public void cancel(Long userId, PaymentCancelRequestDto dto, boolean isRecoveryFlow) {
        log.info("결제 취소 요청 시작 - PaymentId: {}, CancelAmount: {}", dto.paymentId(), dto.cancelAmount());

        PaymentCancelData data = paymentCancelService.readyCancel(userId, dto, isRecoveryFlow);

        if(data == null) {
            return;
        }

        try {
            PaymentCancelResponseDto response = tossBrandpayApiClient.cancelPayment(
                    data.paymentKey(),
                    data.idempotencyKey(),
                    data.cancelAmount().longValueExact(),
                    data.cancelReason()
            );


            paymentPostProcessor.completeCancelPayment(data.paymentCancelId(), response.paymentKey(), data.cancelAmount().longValueExact());


        } catch (PaymentTossApiException e) {
            // 8. 네트워크/타임아웃 오류 처리
            if (e.getErrorCode() == PaymentErrorCode.EXTERNAL_SERVER_ERROR) {
                log.warn("토스 최종 승인 타임아웃 또는 서버 에러 발생 - 상태 유지(PENDING) 및 추후 확인 필요: {}", dto.paymentId());
                paymentPostProcessor.holdCancelPayment(data.paymentCancelId(), e.getErrorCode(), e.getMessage());
                throw e;
            }
            log.error("토스 결제 승인 거절 (4xx): {}", e.getMessage());
            paymentPostProcessor.failCancelPayment(data.paymentCancelId(), e.getErrorCode(), e.getMessage());
            throw e;

        } catch (PaymentSystemException e) {
            log.error("결제 취소 중 예상치 못한 오류 발생 - PaymentId: {}", dto.paymentId(), e);
            paymentPostProcessor.failCancelPayment(data.paymentCancelId(), e.getErrorCode(), e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("결제 취소 중 예상치 못한 오류 발생 - PaymentId: {}", dto.paymentId(), e);
            paymentPostProcessor.holdCancelPayment(data.paymentCancelId(), PaymentErrorCode.SYSTEM_ERROR, e.getMessage());
            throw e;
        }
    }
}

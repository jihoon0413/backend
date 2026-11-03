package com.bubbletea.payment.facade;

import com.bubbletea.payment.entity.PaymentRequestOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.global.exception.PaymentErrorCode;
import com.bubbletea.payment.global.exception.PaymentSystemException;
import com.bubbletea.payment.global.exception.PaymentTossApiException;
import com.bubbletea.payment.repository.PaymentRequestOutboxRepository;
import com.bubbletea.payment.service.PaymentService;
import com.bubbletea.payment.service.dto.PaymentConfirmRequestDto;
import com.bubbletea.payment.service.dto.PaymentConfirmResponseDto;
import com.bubbletea.payment.service.dto.PaymentReadyRequestDto;
import com.bubbletea.payment.service.dto.data.PaymentConfirmData;
import com.bubbletea.payment.service.external.OrderApiClient;
import com.bubbletea.payment.service.external.TossBrandpayApiClient;
import com.bubbletea.payment.processor.PaymentPostProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConfirmFacade {

    private static final String CONFIRM_REQUESTED_TOPIC = "payment.internal.confirm-requested";

    private final PaymentService paymentService;
    private final TossBrandpayApiClient tossBrandpayApiClient;
    private final PaymentPostProcessor paymentPostProcessor;
    private final OrderApiClient orderApiClient;
    private final PaymentRequestOutboxRepository paymentRequestOutboxRepository;
    private final ObjectMapper objectMapper;

    public String ready(PaymentReadyRequestDto dto, Long userId) {

        // TODO: 주문 도메인과 Feign 통신으로 주문/금액 검증을 먼저 수행한다.
        return paymentService.createReadyPayment(dto, userId);
    }

    /**
     * HTTP 요청 스레드에서는 DB 작업만 하고 즉시 반환한다 (Toss 호출은 안 함).
     * READY -> PROCESSING 전이 + outbox 적재가 하나의 트랜잭션으로 묶여서,
     * 결제 결과통지와 분리된 전용 릴레이(PaymentRequestOutboxScheduler)가 Kafka로 중계한다.
     */
    @Transactional
    public Long acceptConfirm(PaymentConfirmRequestDto dto) {
        Long paymentId = paymentService.acceptConfirmRequest(dto.orderId());

        PaymentRequestOutbox outbox = PaymentRequestOutbox.builder()
                .aggregateType("payment")
                .aggregateId(paymentId)
                .topic(CONFIRM_REQUESTED_TOPIC)
                .eventType("confirmRequested")
                .messageKey(dto.orderId())
                .payload(convertToJson(dto))
                .status(OutboxStatus.PENDING)
                .build();

        paymentRequestOutboxRepository.save(outbox);

        return paymentId;
    }

    public void confirm(PaymentConfirmRequestDto dto) {
        // 멱등키 고려
        PaymentConfirmData data = paymentService.getConfirmData(dto.orderId());

        try {
            // confirmBrandpay()가 TimeLimiter 때문에 CompletableFuture를 반환함 - 여기서만 풀어서
            // 아래 분기 처리는 기존과 동일하게 PaymentTossApiException/PaymentSystemException을 직접 받는다.
            PaymentConfirmResponseDto response = resolve(tossBrandpayApiClient.confirmBrandpay(dto, data.idempotencyKey()));

            paymentPostProcessor.completePayment(data.paymentId(), response.paymentKey());

        } catch (PaymentTossApiException e) {
            if (e.getErrorCode() == PaymentErrorCode.EXTERNAL_SERVER_ERROR) {
                log.warn("토스 최종 승인 타임아웃 또는 서버 에러 발생 - 상태 유지(PENDING) 및 추후 확인 필요: {}", dto.orderId());
                paymentPostProcessor.holdPayment(data.paymentId(), e.getErrorCode(), e.getMessage());
            } else {
                log.error("토스 결제 승인 거절 (4xx): {}", e.getMessage());
                paymentPostProcessor.failPayment(data.paymentId(), e.getErrorCode(), e.getMessage());
            }
            throw e;
        } catch (PaymentSystemException e) {
            log.error("시스템 에러 발생: {}", e.getMessage());
            paymentPostProcessor.failPayment(data.paymentId(), e.getErrorCode(), e.getMessage());

            throw e;
        }
    }

    private String convertToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("JSON 변환 실패", e);
        }
    }

    private PaymentConfirmResponseDto resolve(CompletableFuture<PaymentConfirmResponseDto> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PaymentTossApiException ex) {
                throw ex;
            }
            if (cause instanceof PaymentSystemException ex) {
                throw ex;
            }
            throw new PaymentSystemException(PaymentErrorCode.UNAUTHORIZED_ACCESS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PaymentSystemException(PaymentErrorCode.UNAUTHORIZED_ACCESS);
        }
    }
}

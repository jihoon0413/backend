package com.bubbletea.payment.service.external;

import com.bubbletea.payment.config.TossFeignClient;
import com.bubbletea.payment.global.exception.PaymentErrorCode;
import com.bubbletea.payment.global.exception.PaymentSystemException;
import com.bubbletea.payment.global.exception.PaymentTossApiException;
import com.bubbletea.payment.service.dto.PaymentConfirmRequestDto;
import com.bubbletea.payment.service.dto.PaymentConfirmResponseDto;
import com.bubbletea.payment.service.dto.PaymentCancelResponseDto;
import com.bubbletea.payment.service.dto.TossAccessTokenResponseDto;
import com.bubbletea.payment.service.dto.BillingRequestDto;
import com.bubbletea.payment.service.dto.TossRegisteredPaymentMethodsResponseDto;
import com.bubbletea.payment.service.dto.TossStatusResponseDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class TossBrandpayApiClient {

    @Value("${toss.payments.api-secret-key}")
    private String apiSecretKey;

    private final TossFeignClient tossFeignClient;
    // 필드명 = 빈 이름으로 매칭됨 (Executor 타입 빈이 여러 개라 이름으로 구분)
    private final Executor tossCallExecutor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TossAccessTokenResponseDto getAccessToken(String customerKey, String code) {
        String encodedToken = getBasicAuthHeader();

        Map<String, Object> requestData = new HashMap<>();
        requestData.put("grantType", "AuthorizationCode");
        requestData.put("customerKey", customerKey);
        requestData.put("code", code);

        log.info("Toss API Request: getAccessToken");
        try {
            return tossFeignClient.getAccessToken(encodedToken, requestData);
        } catch (FeignException e) {
            try {
                return objectMapper.readValue(e.contentUTF8(), TossAccessTokenResponseDto.class);
            } catch (Exception ex) {
                return new TossAccessTokenResponseDto(null, null, "HTTP_ERROR", e.getMessage());
            }
        }
    }

    public TossRegisteredPaymentMethodsResponseDto getRegisteredPaymentMethods(String accessToken) {
        String bearerToken = "Bearer " + accessToken;

        log.info("Toss API Request: getRegisteredPaymentMethods");
        try {
            return tossFeignClient.getRegisteredPaymentMethods(bearerToken);
        } catch (FeignException e) {
            try {
                return objectMapper.readValue(e.contentUTF8(), TossRegisteredPaymentMethodsResponseDto.class);
            } catch (Exception ex) {
                return new TossRegisteredPaymentMethodsResponseDto(null, null, null, "HTTP_ERROR", e.getMessage());
            }
        }
    }

    /**
     * CircuitBreaker만으론 "진행 중인" 느린 호출 자체를 못 끊는다 - 호출이 끝나야 그 결과를 집계해서
     * 열지 말지 판단하기 때문. TimeLimiter를 얹어서 brandpayTimeLimiter.timeout-duration을 넘기면
     * 강제로 TimeoutException을 던지게 한다. TimeLimiter는 비동기(CompletableFuture) 반환형에만
     * 적용되므로 실제 블로킹 호출은 tossCallExecutor 위에서 돌리고 Future로 감싼다.
     */
    @CircuitBreaker(name = "brandpayCircuitBreaker", fallbackMethod = "confirmBrandpayFallback")
    @TimeLimiter(name = "brandpayTimeLimiter")
    public CompletableFuture<PaymentConfirmResponseDto> confirmBrandpay(PaymentConfirmRequestDto dto, String idempotencyKey) {
        return CompletableFuture.supplyAsync(() -> {
            String encodedToken = getBasicAuthHeader();

            log.info("Brandpay Confirm Request - OrderId: {}, Idempotency: {}", dto.orderId(), idempotencyKey);

            try {
                return tossFeignClient.confirmBrandpay(encodedToken, idempotencyKey, dto);
            } catch (FeignException e) {
                if (e.status() >= 500 || e.status() == 429) {
                    log.warn("토스 API 일시적 오류. 재시도를 트리거합니다. Status: {}", e.status());
                    throw e;
                }
                log.error("Brandpay Confirm Client Error (4xx): {}", e.contentUTF8());
                throw new PaymentTossApiException(PaymentErrorCode.TOSS_PAYMENT_REJECTED);
            } catch (Exception e) {
                throw new PaymentSystemException(PaymentErrorCode.UNAUTHORIZED_ACCESS);
            }
        }, tossCallExecutor);
    }

    public CompletableFuture<PaymentConfirmResponseDto> confirmBrandpayFallback(PaymentConfirmRequestDto dto, String idempotencyKey, Throwable t) {
        if (t instanceof PaymentTossApiException) {
            return CompletableFuture.failedFuture(t);
        }

        if (t instanceof CallNotPermittedException) {
            log.error("🚨 [Fast-Fail] 토스 서버 장애 지속으로 인해 서킷 브레이커가 요청을 즉시 차단함. OrderId: {}", dto.orderId());
        } else if (t instanceof TimeoutException) {
            log.error("⏱ [Timeout] 토스 응답이 제한시간 내에 오지 않아 강제로 실패 처리함. OrderId: {}", dto.orderId());
        } else {
            log.error("단건 결제 최종 실패 - OrderId: {}, Cause: {}", dto.orderId(), t.getMessage());
        }
        return CompletableFuture.failedFuture(new PaymentTossApiException(PaymentErrorCode.EXTERNAL_SERVER_ERROR));
    }

    @Retry(name = "billingRetry", fallbackMethod = "executeBillingFallback")
    public PaymentConfirmResponseDto executeBilling(
            Long paymentId,
            String idempotencyKey,
            BillingRequestDto dto
    ) {
        String encodedToken = getBasicAuthHeader();

        log.info("Toss Billing Request - OrderId: {}, Idempotency: {}, PaymentId: {}",
                dto.orderId(), idempotencyKey, paymentId);

        try {
            //            EXPIRED_CARD, REJECT_CARD_PAYMENT, INSUFFICIENT_BALANCE,
//            CONFIRM_TIMEOUT, INTERNAL_SERVER_ERROR
//            TIMEOUT, REJECT_CARD_COMPANY, INSUFFICIENT_FUNDS
//            String testErrorTriggerCode = "REJECT_CARD_COMPANY";
//            return tossFeignClient.executeBilling(encodedToken, idempotencyKey,testErrorTriggerCode, dto);
            return tossFeignClient.executeBilling(encodedToken, idempotencyKey, dto);

        } catch (FeignException e) {
            if (e.status() >= 500 || e.status() == 429 || e.status() == 408) {
                log.warn("토스 빌링 API 일시적 오류 또는 타임아웃. 재시도를 트리거합니다. Status: {}", e.status());
                throw e;
            }

            // 2. 재시도 불가 대상: 4xx 클라이언트 에러 (한도초과, 잔액부족, 카드만료 등)
            log.error("Toss Billing Client Error (4xx) - 상태 확정 실패 처리: {}", e.contentUTF8());
            throw new PaymentTossApiException(PaymentErrorCode.TOSS_PAYMENT_REJECTED);

        } catch (Exception e) {
            log.error("Toss Billing System Error - 알 수 없는 내부 예외 발생", e);
            throw new PaymentSystemException(PaymentErrorCode.UNAUTHORIZED_ACCESS);
        }
    }

    public PaymentConfirmResponseDto executeBillingFallback(
            Long paymentId,
            String idempotencyKey,
            BillingRequestDto dto,
            FeignException e
    ) {
        log.error("정기결제 최종 재시도 실패 또는 타임아웃 발생 (Fallback 진입) - OrderId: {}, Idempotency: {}, Reason: {}",
                dto.orderId(), idempotencyKey, e.getMessage());

        throw new PaymentTossApiException(PaymentErrorCode.EXTERNAL_SERVER_ERROR);
    }

    @Retry(name = "cancelRetry", fallbackMethod = "cancelPaymentFallback")
    public PaymentCancelResponseDto cancelPayment(
            String paymentKey,
            String idempotencyKey,
            Long cancelAmount,
            String cancelReason
    ) {
        String encodedToken = getBasicAuthHeader();

        log.info("Toss Payment Cancel Request - PaymentKey: {}, Idempotency: {}, CancelAmount: {}",
    paymentKey, idempotencyKey, cancelAmount);

        Map<String, Object> requestData = new HashMap<>();
        requestData.put("cancelReason", cancelReason);
        if (cancelAmount != null) {
            requestData.put("cancelAmount", cancelAmount);
        }

        try {
            return tossFeignClient.cancelPayment(encodedToken, idempotencyKey, paymentKey, requestData);
        } catch (FeignException e) {
            if (e.status() >= 500 || e.status() == 429 || e.status() == 408) {
                log.warn("토스 결제 취소 API 일시적 오류 또는 타임아웃. 재시도를 트리거합니다. Status: {}", e.status());
                throw e;
            }

            log.error("Toss Cancel Client Error (4xx): {}", e.contentUTF8());
            throw new PaymentTossApiException(PaymentErrorCode.TOSS_PAYMENT_REJECTED);
        } catch (Exception e) {
            log.error("Toss Cancel System Error - 알 수 없는 내부 예외 발생", e);
            throw new PaymentSystemException(PaymentErrorCode.UNAUTHORIZED_ACCESS);
        }
    }

    public PaymentCancelResponseDto cancelPaymentFallback(
            String paymentKey,
            String idempotencyKey,
            Long cancelAmount,
            String cancelReason,
            FeignException e
    ) {
        log.error("결제 취소 최종 재시도 실패 또는 타임아웃 발생 (Fallback 진입) - PaymentKey: {}, Reason: {}",
                paymentKey, e.getMessage());
        throw new PaymentTossApiException(PaymentErrorCode.EXTERNAL_SERVER_ERROR);
    }

    public TossStatusResponseDto getTossPaymentStatus(String paymentKey) {
        String encodedToken = getBasicAuthHeader();
        try {
            return tossFeignClient.getPaymentStatus(encodedToken, paymentKey);
        } catch (Exception e) {
            log.error("토스페이먼츠 API 조회 실패 - paymentKey: {}", paymentKey, e);
            throw e;
        }
    }


    private String getBasicAuthHeader() {
        String rawToken = apiSecretKey + ":";
        String encodedToken = Base64.getEncoder().encodeToString(rawToken.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encodedToken;
    }




}

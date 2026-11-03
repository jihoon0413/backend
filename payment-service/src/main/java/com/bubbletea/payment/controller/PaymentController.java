package com.bubbletea.payment.controller;

import com.bubbletea.common.response.ApiResponse;
import com.bubbletea.payment.facade.PaymentCancelFacade;
import com.bubbletea.payment.facade.PaymentConfirmFacade;
import com.bubbletea.payment.service.PaymentService;
import com.bubbletea.payment.service.dto.PaymentCancelRequestDto;
import com.bubbletea.payment.service.dto.PaymentConfirmRequestDto;
import com.bubbletea.payment.service.dto.PaymentStatusResponseDto;
import com.bubbletea.payment.service.dto.data.PaymentCancelData;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentConfirmFacade paymentConfirmFacade;
    private final PaymentCancelFacade paymentCancelFacade;
    private final PaymentService paymentService;

    @PostMapping("/confirm/brandpay")
    public ResponseEntity<ApiResponse<Long>> confirmPayment(@RequestBody PaymentConfirmRequestDto dto) {
        Long paymentId = paymentConfirmFacade.acceptConfirm(dto);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(ApiResponse.success(paymentId, "결제 승인 요청이 접수되었습니다. /status로 처리 결과를 확인하세요."));
    }

    @PostMapping("/cancel")
    public ResponseEntity<ApiResponse<Long>> requestCancel(
            @RequestHeader("X-User-Id") Long userId,
            @RequestBody PaymentCancelRequestDto dto) {
        PaymentCancelData data = paymentCancelFacade.acceptCancel(userId, dto);

        if (data == null) {
            // 이미 취소가 완료된 멱등 요청
            return ResponseEntity.ok(ApiResponse.success(dto.paymentId(), "이미 취소 처리가 완료된 요청입니다."));
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(ApiResponse.success(data.paymentId(), "결제 취소 요청이 접수되었습니다. /status로 처리 결과를 확인하세요."));
    }

    @GetMapping("/{paymentId}/status")
    public ApiResponse<PaymentStatusResponseDto> getPaymentStatus(@PathVariable Long paymentId) {
        return ApiResponse.success(paymentService.getPaymentStatus(paymentId));
    }

//    @PostMapping(path = {"/confirm/widget", "/confirm/payment"}) // 비등록 단건 결제
//    public ApiResponse<String> confirmWidgetPayment(@RequestBody PaymentConfirmRequestDto dto) {
//        paymentService.confirm(dto);
//        return ApiResponse.success(dto.amount() + "원 결제가 완료되었습니다.");
//    }
}

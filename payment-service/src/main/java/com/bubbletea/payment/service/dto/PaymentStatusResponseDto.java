package com.bubbletea.payment.service.dto;

public record PaymentStatusResponseDto(
        Long paymentId,
        String status
) {
}

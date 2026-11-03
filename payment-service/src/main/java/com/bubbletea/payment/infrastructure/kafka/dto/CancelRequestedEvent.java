package com.bubbletea.payment.infrastructure.kafka.dto;

import com.bubbletea.payment.service.dto.PaymentCancelRequestDto;

public record CancelRequestedEvent(
        Long userId,
        PaymentCancelRequestDto request
) {
}

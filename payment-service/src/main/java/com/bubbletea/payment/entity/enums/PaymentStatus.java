package com.bubbletea.payment.entity.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum PaymentStatus {
    READY("결제 대기", "결제 요청을 받아 대기 상태입니다."),
    PROCESSING("승인 처리 중", "컨슈머가 PG 승인을 처리하고 있는 중입니다."),
    PAID("결제 완료", "성공적으로 결제를 완료했습니다."),
    FAILED("결제 실패", "결제 과정에서 오류가 발생했습니다."),
    PARTIALLY_REFUNDED("부분 환불", "결제 금액 중 일부가 환불되었습니다."),
    CANCELLED("결제 취소", "결제가 취소되었습니다."),
    UNKNOWN_HOLD("알 수 없는 상태", "결제(취소)가 성공/실패 했는지 파악할 수 없습니다."),
    CANCEL_UNKNOWN_HOLD("알 수 없는 상태" , "결제 취소가 성공/실패 했는지 파악할 수 없습니다.")
    ;

    private final String status;
    private final String description;
}

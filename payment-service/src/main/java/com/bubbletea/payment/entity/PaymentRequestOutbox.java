package com.bubbletea.payment.entity;

import com.bubbletea.payment.entity.enums.OutboxStatus;
import com.bubbletea.payment.global.common.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * confirm/cancel "접수" 이벤트 전용 outbox - PaymentOutbox(결제 결과를 order/notification-service에
 * 통지하는 용도)와 테이블/스케줄러/릴레이를 완전히 분리한다. 같은 relay를 같이 쓰면 confirm/cancel 접수량이
 * 늘어났을 때 결과 통지 발행까지 같이 밀리고(반대도 마찬가지), 서로 독립적으로 용량을 맞출 수 없기 때문.
 */
@Entity
@Table(name = "payment_request_outbox")
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRequestOutbox extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private Long aggregateId;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String messageKey;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    private String processorId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    public void changeStatus(OutboxStatus outboxStatus) {
        this.status = outboxStatus;
    }
}

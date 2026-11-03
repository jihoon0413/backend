package com.bubbletea.payment.repository;

import com.bubbletea.payment.entity.PaymentRequestOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface PaymentRequestOutboxRepository extends JpaRepository<PaymentRequestOutbox, Long> {

    // confirm/cancel 접수만 처리해서 결과통지보다 건당 발행량이 적다(1건) - 배치는 여유있게 100으로.
    @Modifying
    @Transactional
    @Query(value = "UPDATE payment_request_outbox SET status = 'PROCESSING', processor_id = :processorId, updated_at = :now " +
            "WHERE status = 'PENDING' AND id IN (SELECT id FROM payment_request_outbox WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 100)",
            nativeQuery = true)
    int claimPendingEvents(@Param("processorId") String processorId, @Param("now") LocalDateTime now);

    List<PaymentRequestOutbox> findByStatusAndProcessorId(OutboxStatus status, String processorId);

    @Modifying
    @Transactional
    @Query("UPDATE PaymentRequestOutbox p " +
            "SET p.status = 'PENDING', p.processorId = null, p.updatedAt = :now " +
            "WHERE p.status = 'PROCESSING' AND p.updatedAt < :thresholdTime")
    int cleanupStaleProcessingEvents(
            @Param("now") LocalDateTime now,
            @Param("thresholdTime") LocalDateTime thresholdTime
    );
}

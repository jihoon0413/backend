package com.bubbletea.payment.repository;

import com.bubbletea.payment.entity.PaymentOutbox;
import com.bubbletea.payment.entity.enums.OutboxStatus;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface PaymentOutboxRepository extends JpaRepository<PaymentOutbox, Long> {
    @Modifying
    @Transactional
    // confirm/cancel 접수 이벤트가 같은 relay를 타면서 1건당 outbox 3건(요청1 + 결과통지2)이 생겨
    // 기본 50건/500ms로는 부하 상황에서 처리량이 모자랐다 - 배치 크기를 늘림.
    @Query(value = "UPDATE payment_outbox SET status = 'PROCESSING', processor_id = :processorId, updated_at = :now " +
            "WHERE status = 'PENDING' AND id IN (SELECT id FROM payment_outbox WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 200)",
            nativeQuery = true)
    int claimPendingEvents(@Param("processorId") String processorId, @Param("now") LocalDateTime now);

    List<PaymentOutbox> findByStatusAndProcessorId(OutboxStatus status, String processorId);

    @Modifying
    @Transactional
    @Query("UPDATE PaymentOutbox p " +
            "SET p.status = 'PENDING', p.processorId = null, p.updatedAt = :now " +
            "WHERE p.status = 'PROCESSING' AND p.updatedAt < :thresholdTime")
    int cleanupStaleProcessingEvents(
            @Param("now") LocalDateTime now,
            @Param("thresholdTime") LocalDateTime thresholdTime
    );

}

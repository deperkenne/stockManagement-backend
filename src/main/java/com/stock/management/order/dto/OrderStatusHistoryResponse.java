package com.stock.management.order.dto;

import com.stock.management.order.domain.OrderStatusHistory;

import java.time.Instant;

/** Une entrée de l'historique de statut, telle que renvoyée au client (jamais l'entité JPA). */
public record OrderStatusHistoryResponse(
        String orderId,
        String previousStatus,
        String newStatus,
        String reason,
        Instant changedAt
) {

    public static OrderStatusHistoryResponse from(OrderStatusHistory history) {
        return new OrderStatusHistoryResponse(
                history.getOrderId().toString(),
                history.getPreviousStatus() != null ? history.getPreviousStatus().name() : null,
                history.getNewStatus().name(),
                history.getReason(),
                history.getChangedAt()
        );
    }
}

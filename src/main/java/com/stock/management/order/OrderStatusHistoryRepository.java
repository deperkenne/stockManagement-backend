package com.stock.management.order;

import com.stock.management.order.domain.OrderStatusHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OrderStatusHistoryRepository extends JpaRepository<OrderStatusHistory, Long> {

    /** Reconstitue la timeline complète d'une commande, dans l'ordre chronologique — usage analytique. */
    List<OrderStatusHistory> findByOrderIdOrderByChangedAtAsc(UUID orderId);
}
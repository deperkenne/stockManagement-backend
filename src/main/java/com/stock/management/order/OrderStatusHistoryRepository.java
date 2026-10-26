package com.stock.management.order;

import com.stock.management.order.domain.OrderStatusHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OrderStatusHistoryRepository extends JpaRepository<OrderStatusHistory, Long> {

    /** Reconstitue la timeline complète d'une commande, dans l'ordre chronologique — usage analytique. */
    List<OrderStatusHistory> findByOrderIdOrderByChangedAtAsc(UUID orderId);

    /** Tout l'historique d'un client, toutes commandes confondues, dans l'ordre chronologique. */
    List<OrderStatusHistory> findByCustomerIdOrderByChangedAtAsc(String customerId);

    /** Timeline d'une commande, limitée au client propriétaire : liste vide si la commande n'est pas à lui. */
    List<OrderStatusHistory> findByOrderIdAndCustomerIdOrderByChangedAtAsc(UUID orderId, String customerId);
}

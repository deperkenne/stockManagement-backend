package com.stock.management.order;

import com.stock.management.order.dto.OrderStatusHistoryResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/** Lecture de l'historique des statuts, toujours limitée au client connecté. */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderStatusHistoryService {

    private final OrderStatusHistoryRepository orderStatusHistoryRepository;

    /** Tout l'historique du client, toutes commandes confondues. */
    public List<OrderStatusHistoryResponse> findByCustomer(String customerId) {
        return orderStatusHistoryRepository.findByCustomerIdOrderByChangedAtAsc(customerId).stream()
                .map(OrderStatusHistoryResponse::from)
                .toList();
    }

    /** Historique d'une commande du client : liste vide si la commande appartient à quelqu'un d'autre. */
    public List<OrderStatusHistoryResponse> findByOrderForCustomer(UUID orderId, String customerId) {
        return orderStatusHistoryRepository.findByOrderIdAndCustomerIdOrderByChangedAtAsc(orderId, customerId).stream()
                .map(OrderStatusHistoryResponse::from)
                .toList();
    }
}

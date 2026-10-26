package com.stock.management.order;

import com.stock.management.order.dto.OrderStatusHistoryResponse;
import com.stock.management.shared.security.AuthenticatedCustomer;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * Historique des statuts du client connecté. Le customerId vient toujours du token,
 * jamais d'un paramètre : un client ne peut pas lire l'historique d'un autre.
 */
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderStatusHistoryController {

    private final OrderStatusHistoryService orderStatusHistoryService;

    /** GET /api/orders/history : tout l'historique du client connecté. */
    @GetMapping("/history")
    public List<OrderStatusHistoryResponse> getMyHistory(@AuthenticationPrincipal Jwt jwt) {
        return orderStatusHistoryService.findByCustomer(AuthenticatedCustomer.idOf(jwt));
    }

    /** GET /api/orders/{orderId}/history : historique d'une commande du client connecté. */
    @GetMapping("/{orderId}/history")
    public List<OrderStatusHistoryResponse> getMyOrderHistory(@AuthenticationPrincipal Jwt jwt,
                                                              @PathVariable UUID orderId) {
        return orderStatusHistoryService.findByOrderForCustomer(orderId, AuthenticatedCustomer.idOf(jwt));
    }
}

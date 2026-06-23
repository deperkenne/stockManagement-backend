package com.stock.management.order;

import com.stock.management.order.dto.*;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateOrderResponse receiveOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("[REST] POST /api/orders externalOrderNr={}", request.externalOrderNr());
        return orderService.receiveOrder(request);
    }

    /** Annule toute la commande et libère tout le stock alloué. */
    @PostMapping("/{orderId}/cancel")
    public CancelOrderResponse cancelOrder(@PathVariable UUID orderId,
                                            @Valid @RequestBody CancelOrderRequest request) {
        log.info("[REST] POST /api/orders/{}/cancel by={}", orderId, request.cancelledBy());
        return orderService.cancelOrder(orderId, request);
    }

    /** Annule une seule ligne et libère uniquement le stock alloué à cette ligne. */
    @PostMapping("/{orderId}/lines/{lineItemId}/cancel")
    public CancelOrderResponse cancelLineItem(@PathVariable UUID orderId,
                                               @PathVariable UUID lineItemId,
                                               @Valid @RequestBody CancelOrderRequest request) {
        log.info("[REST] POST /api/orders/{}/lines/{}/cancel by={}", orderId, lineItemId, request.cancelledBy());
        return orderService.cancelLineItem(orderId, lineItemId, request);
    }
}
package com.stock.management.order.dto;

import java.time.Instant;
import java.util.List;

/** Réponse renvoyée par GET /api/orders et GET /api/orders/{orderId}. */
public record OrderResponse(
        String orderId,
        String externalOrderNr,
        String customerId,
        String status,
        String priority,
        boolean completeDeliveryRequired,
        String currency,
        Instant receivedAt,
        List<LineItemResponse> lineItems
) {}
package com.stock.management.order.dto;

import java.time.Instant;

public record CreateOrderResponse(
        String orderId,
        String externalOrderNr,
        String status,
        int lineItemCount,
        Instant receivedAt
) {}
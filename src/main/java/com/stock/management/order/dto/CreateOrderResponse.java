package com.stock.management.order.dto;

import java.time.Instant;

public record CreateOrderResponse(
        String orderId,
        String status,
        int lineItemCount,
        Instant receivedAt
) {}

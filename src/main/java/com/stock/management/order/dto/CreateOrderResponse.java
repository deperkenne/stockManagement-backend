package com.stock.management.order.dto;

import java.time.Instant;

public record CreateOrderResponse(
        String orderId,
        String customerId,
        String status,
		String priority,
		boolean completeDeliveryRequired,
        int lineItemCount,
        Instant receivedAt
) {}

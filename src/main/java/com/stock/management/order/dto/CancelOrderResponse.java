package com.stock.management.order.dto;

import java.time.Instant;

public record CancelOrderResponse(
        String orderId,
        String status,
        int releasedAllocations,
        String cancellationSource,
        Instant cancelledAt
) {}
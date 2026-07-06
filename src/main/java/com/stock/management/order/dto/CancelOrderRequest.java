package com.stock.management.order.dto;

import com.stock.management.order.domain.CancellationSource;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.UUID;

public record CancelOrderRequest(

        @NotBlank(message = "reason must not be blank")
        String reason,

        @NotBlank(message = "cancelledBy must not be blank")
        String cancelledBy,

        @NotNull(message = "cancellationSource must not be null")
        CancellationSource cancellationSource,

		List<UUID>lineItemIds
) {}

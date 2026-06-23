package com.stock.management.order.dto;

import com.stock.management.order.domain.Priority;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

public record CreateOrderRequest(

        @NotBlank(message = "externalOrderNr must not be blank")
        String externalOrderNr,

        @NotBlank(message = "customerId must not be blank")
        String customerId,

        @NotBlank(message = "warehouseId must not be blank")
        String warehouseId,

        @NotNull(message = "priority must not be null")
        Priority priority,

        boolean completeDeliveryRequired,

        @NotBlank(message = "currency must not be blank")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO code")
        String currency,

        @NotEmpty(message = "lineItems must not be empty")
        @Valid
        List<LineItemRequest> lineItems
) {}
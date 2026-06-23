package com.stock.management.order.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record LineItemRequest(

        @NotBlank(message = "productNr must not be blank")
        String productNr,

        @Positive(message = "requestedQty must be greater than 0")
        int requestedQty,

        @NotNull(message = "unitPrice must not be null")
        @DecimalMin(value = "0.00", message = "unitPrice must not be negative")
        BigDecimal unitPrice
) {}
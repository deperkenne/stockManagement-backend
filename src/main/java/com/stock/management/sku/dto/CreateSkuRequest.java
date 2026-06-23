package com.stock.management.sku.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record CreateSkuRequest(

        @NotBlank(message = "productNr must not be blank")
        String productNr,

        @Positive(message = "totalQuantity must be greater than 0")
        int totalQuantity,

        @NotBlank(message = "locationCode must not be blank")
        String locationCode
) {}
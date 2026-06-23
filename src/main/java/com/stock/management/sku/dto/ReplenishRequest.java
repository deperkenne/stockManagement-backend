package com.stock.management.sku.dto;

import jakarta.validation.constraints.Positive;

public record ReplenishRequest(
        @Positive(message = "quantity must be positive")
        int quantity
) {}
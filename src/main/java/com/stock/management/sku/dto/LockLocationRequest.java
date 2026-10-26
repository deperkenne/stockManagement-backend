package com.stock.management.sku.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** Corps JSON de POST /api/v1/locations/{id}/lock : { "reason": "Inventaire" } */
public record LockLocationRequest(
        @NotBlank(message = "reason must not be blank")
        @Size(max = 255, message = "reason must not exceed 255 characters")
        String reason
) {}

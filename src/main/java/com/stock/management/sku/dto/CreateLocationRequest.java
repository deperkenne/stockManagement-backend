package com.stock.management.sku.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** Corps JSON de POST et PUT /api/v1/locations : { "code": "A-01" } */
public record CreateLocationRequest(
        @NotBlank(message = "code must not be blank")
        @Size(max = 50, message = "code must not exceed 50 characters")
        String code
) {}

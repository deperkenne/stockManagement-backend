package com.stock.management.order.dto;

import com.stock.management.order.domain.Priority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Corps JSON pour PUT /api/orders/{orderId}.
 *
 * Exemple à coller dans Postman :
 * {
 *   "priority": "HIGH",
 *   "completeDeliveryRequired": false,
 *   "currency": "EUR"
 * }
 *
 * Volontairement limité à ces 3 champs : externalOrderNr (identifiant métier unique) et status
 * (piloté par la state machine interne / Kafka) ne doivent jamais être modifiés à la main via ce endpoint.
 */
public record UpdateOrderRequest(

        @NotNull(message = "priority must not be null")
        Priority priority,

        boolean completeDeliveryRequired,

        @NotBlank(message = "currency must not be blank")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO code")
        String currency
) {}
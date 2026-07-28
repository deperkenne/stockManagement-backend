package com.stock.management.allocationLine;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.UUID;

/**
 * Corps JSON attendu pour CREER (POST) ou MODIFIER (PUT) une AllocationItem.
 *
 * Exemple à coller dans Postman :
 * {
 *   "orderId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "lineItemId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "productNr": "ABC123",
 *   "skuId": null,
 *   "quantity": 10,
 *   "remainingQuantity": 0,
 *   "status": "ALLOCATED"
 * }
 *
 * "status" accepte uniquement : ALLOCATED, WAITING_STOCK, NOT_ALLOCATED.
 * "skuId" peut être null (cas NOT_ALLOCATED / WAITING_STOCK : aucun stock trouvé).
 * L'id technique (Long, auto-généré) ne fait jamais partie de ce corps : il vient de l'URL.
 */
public record AllocationItemRequest(

        @NotNull(message = "orderId ne doit pas être vide")
        UUID orderId,

        @NotNull(message = "lineItemId ne doit pas être vide")
        UUID lineItemId,

        @NotBlank(message = "productNr ne doit pas être vide")
        String productNr,

        UUID skuId,

        @PositiveOrZero(message = "quantity doit être positif ou nul")
        int quantity,

        @PositiveOrZero(message = "remainingQuantity doit être positif ou nul")
        int remainingQuantity,

        @NotNull(message = "status ne doit pas être vide (ALLOCATED, WAITING_STOCK ou NOT_ALLOCATED)")
        AllocationItemStatus status
) {}

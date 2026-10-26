package com.stock.management.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record LineItemRequest(

        @NotBlank(message = "productNr must not be blank")
        String productNr,

        @Positive(message = "requestedQty must be greater than 0")
		Integer requestedQty,

        /**
         * Ignoré à la réception d'une commande : OrderService utilise le prix catalogue du Product.
         * Conservé pour la compatibilité des clients existants (champ optionnel).
         */
        BigDecimal unitPrice
) {}

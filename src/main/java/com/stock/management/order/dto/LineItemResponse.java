package com.stock.management.order.dto;

import java.math.BigDecimal;

/** Représente une ligne de commande dans les réponses JSON (jamais l'entité JPA directement). */
public record LineItemResponse(
        String lineItemId,
        String productNr,
        int requestedQty,
        int allocatedQty,
        BigDecimal unitPrice,
        String status
) {}
package com.stock.management.sku.dto;

/** occupied = true si un SKU est stocké à cet emplacement. */
public record LocationResponse(
        String locationId,
        String code,
        boolean locked,
        String lockedReason,
        boolean occupied
) {}

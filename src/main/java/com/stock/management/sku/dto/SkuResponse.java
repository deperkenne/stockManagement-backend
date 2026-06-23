package com.stock.management.sku.dto;

public record SkuResponse(
        String skuId,
        String productNr,
        int totalQuantity,
        int availableQuantity,
        String locationCode,
        boolean locationLocked,
        String locationLockedReason
) {}
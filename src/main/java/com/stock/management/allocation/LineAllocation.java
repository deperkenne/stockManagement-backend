package com.stock.management.allocation;

import com.stock.management.sku.domain.Sku;

public record LineAllocation(Sku sku, int qty) {
}

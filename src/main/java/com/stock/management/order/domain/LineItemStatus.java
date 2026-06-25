package com.stock.management.order.domain;

public enum LineItemStatus {
    PENDING,
    PARTIALLY_ALLOCATED,
    FULLY_ALLOCATED,
	NOT_ALLOCATED,
    CANCELLED
}

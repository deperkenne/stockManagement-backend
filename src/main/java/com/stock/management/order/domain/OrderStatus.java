package com.stock.management.order.domain;

public enum OrderStatus {
    PENDING,
	PENDIND_STOCK,
    IN_PROGRESS,
    PARTIALLY_ALLOCATED,
    FULLY_ALLOCATED,
	ALLOCATION_FAILED,
    CANCELLED,
}

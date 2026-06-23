package com.stock.management.order.domain;

public enum OrderStatus {
    PENDING,
    IN_PROGRESS,
    PARTIALLY_ALLOCATED,
    FULLY_ALLOCATED,
    CANCELLED,
    COMPLETED
}
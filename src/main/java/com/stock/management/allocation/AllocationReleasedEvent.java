package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;

import java.util.List;

public record AllocationReleasedEvent(List<AllocationItem> allocationItems) {
}

package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.order.domain.LineItem;

import java.util.Optional;
import java.util.UUID;

public record RetryRecord(UUID orderId,
						  UUID lineItemId,
						  String productNr,
						  int remainingQuantity,
						  AllocationItem existingAllocationItem) {

		static RetryRecord fromAllocationItem(AllocationItem item) {
			return new RetryRecord(
				item.getOrderId(), item.getLineItemId(),
				item.getProductNr().toString(), item.getRemainingQuantity(),
				item
			);
		}

		static RetryRecord fromNeverAllocatedLine(UUID orderId, LineItem line) {
			return new RetryRecord(
				orderId, line.getId().getValue(),
				line.getProductNr().getValue(), line.getRequestedQty().getValue(),
				null
			);
		}

}

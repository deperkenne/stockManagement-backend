package com.stock.management.allocation;

import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.sku.domain.Sku;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Helper {

	public static List<LineAllocation> greedyAllocate(List<Sku> skus, int needed) {
		List<LineAllocation> result = new ArrayList<>();
		int remaining = needed;
		for (Sku sku : skus) {
			if (remaining <= 0) break;
			int take = Math.min(remaining, sku.getAvailableQuantity().getValue());
			if (take > 0) {
				result.add(new LineAllocation(sku, take));
				remaining -= take;
			}
		}
		return result;
	}

}

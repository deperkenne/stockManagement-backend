package com.stock.management.allocationLine;

import com.stock.management.kafka.event.StockAllocatedEvent;
import com.stock.management.order.domain.ProductNr;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class AllocationItemService {
	private final AllocationItemRepository allocationRepo;

	private final AllocationItemRepository allocationItemRepository;

	public void onStockAllocated(StockAllocatedEvent event) {
		log.info("[ORDER-MODULE] Received StockAllocatedEvent for Order: {}", event.getOrderId());

		UUID orderIdObj = UUID.fromString(event.getOrderId());
		UUID lineItemIdObj = UUID.fromString(event.getOrderId());
		int totalLines = event.getAllocatedLines().size();

		List<StockAllocatedEvent.AllocatedLine> lines = event.getAllocatedLines();

        if(totalLines == 0){
			return;
		}
		for (StockAllocatedEvent.AllocatedLine line : lines) {

			/**
			 * 2. ENREGISTREMENT DANS LA TABLE allocation_items (OPTION B)
			 */
			AllocationItem allocationItem = AllocationItem.builder()
				.orderId(orderIdObj)
				.lineItemId( UUID.fromString(line.getLineItemId()))
				.productNr(new ProductNr(line.getProductNr()))
				.quantity(line.getQuantityAllocated())
				.remainingQuantity(line.getRemainingQuantity())
				.skuId(UUID.fromString(line.getSku()))

				.build();


			allocationItemRepository.save(allocationItem);

		}

		log.info("[ORDER-MODULE] Order {} status synchronized with allocation results", event.getOrderId());
	}

	private AllocationItemStatus mapToDomainStatus(StockAllocatedEvent.AllocatedStatus eventStatus) {
		return switch (eventStatus) {
			case ALLOCATED -> AllocationItemStatus.ALLOCATED;
			case WAITING_STOCK -> AllocationItemStatus.WAITING_STOCK;
			case NOT_ALLOCATED -> AllocationItemStatus.NOT_ALLOCATED;
		};
	}

}

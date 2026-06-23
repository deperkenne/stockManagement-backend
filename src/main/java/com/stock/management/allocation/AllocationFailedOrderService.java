package com.stock.management.allocation;

import com.stock.management.kafka.event.AllocationFailedEvent;
import com.stock.management.order.domain.OrderId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
@Slf4j
@Service
@RequiredArgsConstructor
public class AllocationFailedOrderService {
	private final AllocationFailedOrderRepo allocationFailedOrderRepo;

	@Transactional
	// recoit le message
	public void saveFailure(AllocationFailedEvent event) {
		log.info("[EXPERT-LOG] Saving allocation failure for order: {}", event.getOrderId());
		AllocationFailedOrder.FailureReason reason =AllocationFailedOrder.FailureReason.INSUFFICIENT_STOCK;
		// Traduction de la raison de l'événement vers notre énumération DB
		if(event.getFailureReason() == AllocationFailedEvent.FailureReason.PARTIAL_STOCK){
			reason = AllocationFailedOrder.FailureReason.PARTIAL_STOCK;
		}

		AllocationFailedOrder failedOrder = AllocationFailedOrder.builder()
			.id(new OrderId(UUID.fromString(event.getOrderId())))
			.failureReason(reason)
			.status(AllocationFailedOrder.ProcessedStatus.PENDING)
			.build();

		// Mapping des lignes de l'événement vers les entités JPA
		event.getFailedLines().forEach(line -> failedOrder.addLine(
			FailedLine.builder()
				.sku(line.getOrderId())
				.quantityNeeded(line.getRequestedQuantity())
				.quantityAllocated(line.getAllocatedQuantity())
				.stockAvailableAtFailure(line.getAvailableQuantity())
				.shortageQuantity(line.getShortageQuantity())
				.build()
		));

		allocationFailedOrderRepo.save(failedOrder);
		log.info("[EXPERT-LOG] Order {} successfully persist in dead-letter table.", event.getOrderId());
	}

	@Transactional
	public void processReplayForSkus(List<String> modifiedSkus) {
		log.info("[REPLAY] Stock modified for SKUs: {}. Looking for stuck orders...", modifiedSkus);

		List<AllocationFailedOrder> ordersToReplay = allocationFailedOrderRepo.findPendingOrdersBySkus(modifiedSkus, AllocationFailedOrder.ProcessedStatus.PENDING);

		if (ordersToReplay.isEmpty()) {
			log.info("[REPLAY] No stuck orders found for those SKUs.");
			return;
		}

		for (AllocationFailedOrder order : ordersToReplay) {
			order.setStatus(AllocationFailedOrder.ProcessedStatus.REPLAYING);
			allocationFailedOrderRepo.save(order);

			// 🚀 ICI : Tu publies un événement `order-retry-allocation-topic` pour que le Stock-Service réessaie
			log.info("[REPLAY] Dispatching order {} to retry pipeline", order.getId().getValue());
			// kafkaPublisher.sendRetry(order.getId().getValue().toString());
		}
	}
}

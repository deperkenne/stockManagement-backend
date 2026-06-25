package com.stock.management.kafka.handler;

import com.stock.management.allocation.AllocationService;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.kafka.event.*;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.Quantity;
import com.stock.management.sku.SkuService;
import com.stock.management.sku.domain.SkuId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventHandler {

    private final AllocationService allocationService;
    //private final AllocationRetryService retryService;
    private final SkuService skuService;
	private final OrderService orderService;
	private final AllocationItemService allocationItemService;
    private final KafkaEventPublisher kafkaEventPublisher;

    public void handleOrderReceived(OrderReceivedEvent event) {
        log.info("[HANDLER] order.received orderId={} customerId={} lines={}",
                event.getOrderId(), event.getCustomerId(), event.getLines().size());
        allocationService.allocate(event);
    }

    public void handleOrderReceivedBatch(List<OrderReceivedEvent> events) {
        log.info("[HANDLER] order.received batch size={}", events.size());
		// before we start the process allocation we need to send a message
		// to order to change the order status
        allocationService.allocateBatch(events);
    }

    public void handleStockAllocated(StockAllocatedEvent event) {
        log.info("[HANDLER] stock.allocated orderId={} lines={}",
                event.getOrderId(), event.getAllocatedLines().size());
        // TODO: confirm order status, notify downstream ERP
		allocationItemService.onStockAllocated(event);
    }

    /**
     * Annulation reçue → libère le stock de chaque allocation → publie StockReleasedEvent.
     * StockReleasedEvent déclenchera notifyStockAvailable() pour les commandes en attente.
     */
    public void handleOrderCancelled(OrderCancelledEvent event) {
        log.info("[HANDLER] order.cancelled orderId={} scope={} allocations={}",
                event.getOrderId(), event.getScope(), event.getAllocations().size());

        if (event.getAllocations() == null || event.getAllocations().isEmpty()) {
            log.debug("[HANDLER] No allocations to release for orderId={}", event.getOrderId());
            return;
        }
		if(!event.isCompleteDeliveryRequired()) {
			// Libérer le stock pour chaque allocation
			// la liberation doit se passer dans la table stock allocation
			// tres important a reflechir pour la liberation
			List<StockReleasedEvent.ReleasedLine> releasedLines = event.getAllocations().stream()
				.map(alloc -> {
					skuService.release(new SkuId(alloc.getSkuId()), new Quantity(alloc.getQuantity()));
					return StockReleasedEvent.ReleasedLine.builder()
						.sku(alloc.getProductNr())
						.quantityReleased(alloc.getQuantity())
						.locationId(alloc.getLocationId())
						.build();
				})
				.toList();



			// Publier StockReleasedEvent → déclenche notifyStockAvailable() dans handleStockReleased()
			kafkaEventPublisher.publishStockReleased(StockReleasedEvent.builder()
				.eventId(UUID.randomUUID().toString())
				.orderId(event.getOrderId())
				.releaseReason(event.getScope().name() + " — " + event.getReason())
				.releasedLines(releasedLines)
				.occurredAt(Instant.now())
				.build());

			log.info("[HANDLER] Released {} allocations for orderId={}", releasedLines.size(), event.getOrderId());
		}
    }

    /**
     * Stock libéré → notifier le retry pour chaque productNr libéré.
     * line.getSku() contient le productNr — plusieurs SKUs peuvent le couvrir.
     */
    public void handleStockReleased(StockReleasedEvent event) {
        log.info("[HANDLER] stock.released orderId={} lines={}",
                event.getOrderId(), event.getReleasedLines().size());

        event.getReleasedLines().forEach(line -> {
            log.debug("[HANDLER] Notifying retry — productNr={} qty={}", line.getSku(), line.getQuantityReleased());
           // retryService.notifyStockAvailable(line.getSku());
        });
    }

    public void handleSkuCorrected(SkuCorrectedEvent event) {
        log.info("[HANDLER] sku.corrected orderId={} {} -> {}",
                event.getOrderId(), event.getOldSku(), event.getCorrectedSku());
        // TODO: re-run allocation with corrected SKU
    }

    public void handleAllocationFailed(AllocationFailedEvent event) {
        log.warn("[HANDLER] allocation.failed orderId={} ",
                event.getOrderId());
		orderService.handleCompleteDeliveryFailure(UUID.fromString(event.getOrderId()));

    }

    public void handleSkuSubstituted(SkuSubstitutedEvent event) {
        log.info("[HANDLER] sku.substituted orderId={} substitutions={}",
                event.getOrderId(), event.getSubstitutions().size());
        // TODO: notify customer if not already done
    }

    public void handleOrderDeallocated(OrderDeallocatedEvent event) {
        log.info("[HANDLER] order.deallocated orderId={} reason={}",
                event.getOrderId(), event.getReason());
        // TODO: update warehouse availability
    }
}

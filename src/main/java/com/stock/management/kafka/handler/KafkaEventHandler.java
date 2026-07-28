package com.stock.management.kafka.handler;

import com.stock.management.allocation.AllocationRetryService;
import com.stock.management.allocation.AllocationService;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.kafka.event.*;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.CustomerOrder;
import com.stock.management.order.domain.OrderId;
import com.stock.management.sku.SkuService;
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

	/*
    private final AllocationRetryService allocationRetryService;
    private final SkuService skuService;
	private final OrderService orderService;
	*/

	private final AllocationItemService allocationItemService;
    private final KafkaEventPublisher kafkaEventPublisher;



    public void handleOrderReceivedBatch(List<OrderReceivedEvent> orderReceivedEvents) {
        log.info("[HANDLER] order.received batch size={}", orderReceivedEvents.size());
		// before we start the process allocation we need to send a message
		// to order to change the order status
        allocationService.allocate(orderReceivedEvents);
    }

	/*
	public void handleOrderReceived(OrderReceivedEvent event) {
		log.info("[HANDLER] order.received orderId={} customerId={} lines={}",
			event.getOrderId(), event.getCustomerId(), event.getLines().size());
		// allocationService.allocate(event);
	}

    public void handleStockAllocated(StockAllocatedEvent event) {
        log.info("[HANDLER] stock.allocated orderId={} lines={}",
                event.getOrderId(), event.getAllocatedLines().size());
        // TODO: confirm order status, notify downstream ERP
		allocationItemService.onStockAllocated(event);
    }


     // Annulation reçue → libère le stock de chaque allocation → publie StockReleasedEvent.
     //StockReleasedEvent déclenchera notifyStockAvailable() pour les commandes en attente.

    public void handleOrderCancelled(OrderCancelledEvent event) {
        log.info("[HANDLER] order.cancelled orderId={} scope={} allocations={}",
                event.getOrderId(), event.getScope(), event.getAllocations().size());

        if (event.getAllocations() == null || event.getAllocations().isEmpty()) {
            log.debug("[HANDLER] No allocations to release for orderId={}", event.getOrderId());
            return;
        }

        handleStockReleased(toStockReleasedEvent(event));
    }

    private StockReleasedEvent toStockReleasedEvent(OrderCancelledEvent event) {
        List<StockReleasedEvent.ReleasedLine> lines = event.getAllocations().stream()
            .map(a -> StockReleasedEvent.ReleasedLine.builder()
                .sku(a.getSkuId().toString())
                .ProductNr(a.getProductNr())
                .quantityAllocated(a.getQuantity())
                .locationId(a.getLocationId())
                .build())
            .toList();

        return StockReleasedEvent.builder()
            .eventId(UUID.randomUUID().toString())
            .orderId(event.getOrderId())
            .releasedLines(lines)
            .occurredAt(Instant.now())
            .build();
    }


     //Stock libéré → notifier le retry pour chaque productNr libéré.
     //line.getSku() contient le productNr — plusieurs SKUs peuvent le couvrir.

    public void handleStockReleased(StockReleasedEvent event) {
        log.info("[HANDLER] stock.released orderId={} lines={}",
                event.getOrderId(), event.getReleasedLines().size());
        //skuService.releaseBulkStock(event);
        //allocationRetryService.retryPendingOrders(event);
    }

    public void handleSkuCorrected(SkuCorrectedEvent event) {

        log.info("[HANDLER] sku.corrected orderId={} {} -> {}",
                event.getOrderId(), event.getOldSku(), event.getCorrectedSku());
        // TODO: re-run allocation with corrected SKU
    }

    public void handleAllocationFailed(AllocationFailedEvent event) {
        log.warn("[HANDLER] allocation.failed orderId={} ",
                event.getOrderId());
		orderService.handleCompleteDeliveryFailure(new OrderId(UUID.fromString(event.getOrderId())));  // before we send request to a db check if this order already exist
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

	 */

}

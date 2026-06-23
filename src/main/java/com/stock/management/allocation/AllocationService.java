package com.stock.management.allocation;

import com.stock.management.kafka.event.AllocationFailedEvent;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockAllocatedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.OrderId;
import com.stock.management.order.domain.OrderStatus;
import com.stock.management.order.domain.Priority;
import com.stock.management.order.domain.Quantity;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.domain.Sku;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AllocationService {
    private final OrderRepository orderRepository;
    private final SkuRepository skuRepository;
    private final KafkaEventPublisher kafkaEventPublisher;
    //private final AllocationRetryService retryService;

    // ─── Entrée batch ─────────────────────────────────────────────────────────────
    //
    // 1 seul appel DB pour tout le batch → traitement en mémoire → flush unique.
    // Priorité : HIGH consomme le stock avant NORMAL/LOW dans le même batch.
    // Anti-deadlock : findAvailableSkusForAllocationWithLock trie par productNr ASC.


    @Transactional
    public void allocateBatch(List<OrderReceivedEvent> events) {
		log.info("start order allocation");
        orderChangeStatus(events,OrderStatus.IN_PROGRESS);
		//kafkaEventPublisher.publishOrderINPROGRESS(events);
        log.info("[ALLOC] batch size={}", events.size());

        List<String> skuCodes = events.stream()
                .flatMap(e -> e.getLines().stream())
                .map(OrderReceivedEvent.OrderLine::getSku)
                .distinct()
			    .sorted()
                .collect(Collectors.toList());

        List<Sku> allSkus = skuRepository.findAvailableSkusForAllocationWithLock(skuCodes);

        Map<String, List<Sku>> stockMap = allSkus.stream()
                .collect(Collectors.groupingBy(s -> s.getProductNr().getValue()));

        List<OrderReceivedEvent> sorted = events.stream()
                .sorted(Comparator.comparingInt(
                        (OrderReceivedEvent e) -> priorityOrdinal(e.getPriority())).reversed())
                .collect(Collectors.toList());

        for (OrderReceivedEvent event : sorted) {
            processOrder(event, stockMap);
        }
    }

    // ─── Entrée unitaire ──────────────────────────────────────────────────────────

    @Transactional
    public void allocate(OrderReceivedEvent event) {
        log.info("[ALLOC] single orderId={} priority={} completeDelivery={}",
                event.getOrderId(), event.getPriority(), event.isCompleteDeliveryRequired());

        List<String> skuCodes = event.getLines().stream()
                .map(OrderReceivedEvent.OrderLine::getSku)
                .distinct()
                .collect(Collectors.toList());

        List<Sku> skus = skuRepository.findAvailableSkusForAllocationWithLock(skuCodes);

        Map<String, List<Sku>> stockMap = skus.stream()
                .collect(Collectors.groupingBy(s -> s.getProductNr().getValue()));

        processOrder(event, stockMap);
    }


	private void orderChangeStatus(List<OrderReceivedEvent> events, OrderStatus orderStatus){
		List<OrderId> domainOrderIds = events.stream()
			.map(OrderReceivedEvent::getOrderId)
			.map(UUID::fromString)
			.map(OrderId::new)
			.collect(Collectors.toList());
		orderRepository.updateStatusForIds(domainOrderIds,orderStatus);
	}

    // ─── Logique d'allocation (partagée batch / unitaire) ─────────────────────────

    private void processOrder(OrderReceivedEvent event, Map<String, List<Sku>> stockMap) {

        // Vérification tout-ou-rien AVANT toute réservation
        if (event.isCompleteDeliveryRequired()) {
            Optional<OrderReceivedEvent.OrderLine> insufficient = event.getLines().stream()
                    .filter(l -> totalAvailable(stockMap.get(l.getSku())) < l.getQuantity())
                    .findFirst();

            if (insufficient.isPresent()) {
                OrderReceivedEvent.OrderLine line = insufficient.get();
                int available = totalAvailable(stockMap.get(line.getSku()));
                log.warn("[ALLOC] CompleteDelivery impossible — orderId={} productNr={} needed={} available={}",
                        event.getOrderId(), line.getSku(), line.getQuantity(), available);

				// 🟢 CORRECTION 1 : On transforme TOUTES les lignes de la commande en lignes "Échouées"
				// Car l'utilisateur doit savoir que l'ensemble de sa commande a été bloqué
				List<AllocationFailedEvent.FailedLine> allFailedLines = event.getLines().stream()
					.map(orderLine ->  {
						int avail = totalAvailable(stockMap.get(orderLine.getSku()));
						// On marque chaque ligne comme non allouée (0) avec le stock actuellement dispo
						//return new AllocationFailedEvent.FailedLine(orderLine.getSku(), orderLine.getQuantity(), 0, avail);
						return AllocationFailedEvent.FailedLine.builder()
							.productId(orderLine.getSku())
							.orderId(orderLine.getOrderId())
							.requestedQuantity(orderLine.getQuantity())
							.availableQuantity(avail)
							.allocatedQuantity(0)
							.shortageQuantity(0)
							.build();
					})
					.toList();

                publishAllocationFailed(event,
                        allFailedLines,
                        AllocationFailedEvent.FailureReason.INSUFFICIENT_STOCK);

                //retryService.scheduleRetry(event,
				//AllocationFailedEvent.FailureReason.INSUFFICIENT_STOCK,
                //        List.of(line.getSku()));

                return;
            }
        }
		/**
		 * toutes ses liste doivent etre creer dans des methodes privee afin de respecter le S  SOLID
		 */
		List<StockAllocatedEvent.AllocatedLine> allocated = new ArrayList<>();
        List<AllocationFailedEvent.FailedLine> failed = new ArrayList<>();
        List<String> failedProductNrs = new ArrayList<>();

        for (OrderReceivedEvent.OrderLine line : event.getLines()) {
            List<Sku> skus = stockMap.getOrDefault(line.getSku(), List.of());
            List<LineAllocation> lineAllocs = greedyAllocate(skus, line.getQuantity());
            int totalAllocated = lineAllocs.stream().mapToInt(LineAllocation::qty).sum();
            // permet de verifier si une ligne a ete allouer
			// car toutes les ligne d'une commande peuvent ne pas etre allouer
			// car pas de stock disponible malgre partialdelevrery
            if (totalAllocated == 0) {
				int availableQuantity = totalAvailable(stockMap.get(line.getSku()));
                failed.add(toFailedLine(line, availableQuantity,totalAllocated)); // ligne pas allouer a inserer dans la table de AllocationRetry
                failedProductNrs.add(line.getSku());
                continue;
            }

            for (LineAllocation la : lineAllocs) {
                la.sku().reserve(new Quantity(la.qty()));
                allocated.add(StockAllocatedEvent.AllocatedLine.builder()  // DRY
                        .sku(line.getSku())
                        .quantityAllocated(la.qty())
                        .locationId(la.sku().getLocation().getCode())
                        .build());
            }

			// verifier si une ligne n'a pas totalement ete allouer  dans ce cas insertion de la ligne avec le reste qui n'a pas ete allouer
			// dans la liste linefailed
            if (totalAllocated < line.getQuantity()) {
                log.warn("[ALLOC] Partial — orderId={} sku={} needed={} got={}",
                        event.getOrderId(), line.getSku(), line.getQuantity(), totalAllocated); // DRY
                failed.add(AllocationFailedEvent.FailedLine.builder()
					.productId(line.getSku())
					.orderId(line.getOrderId())
					.requestedQuantity(line.getQuantity())
					.availableQuantity(totalAllocated)
					.allocatedQuantity(totalAllocated)
					.shortageQuantity(line.getQuantity()-totalAllocated)
					.build());
                //failedProductNrs.add(line.getSku());

            }
        }

        if (allocated.isEmpty()) {
            publishAllocationFailed(event, failed, AllocationFailedEvent.FailureReason.INSUFFICIENT_STOCK);
			//retryService.scheduleRetry(event,
			//AllocationFailedEvent.FailureReason.INSUFFICIENT_STOCK, failedProductNrs);

        } else {
            publishStockAllocated(event, allocated);
           // retryService.markResolved(event.getOrderId());

            if (!failed.isEmpty()) {
                publishAllocationFailed(event, failed, AllocationFailedEvent.FailureReason.PARTIAL_STOCK);
				//retryService.scheduleRetry(event,
				//	AllocationFailedEvent.FailureReason.PARTIAL_STOCK, failedProductNrs);
            }
        }
    }

    // ─── Algorithme glouton ───────────────────────────────────────────────────────

    private List<LineAllocation> greedyAllocate(List<Sku> skus, int needed) {
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

    // ─── Helpers ─────────────────────────────────────────────────────────────────

    private int totalAvailable(List<Sku> skus) {
        if (skus == null) return 0;
        return skus.stream().mapToInt(s -> s.getAvailableQuantity().getValue()).sum();
    }

    private int priorityOrdinal(String priority) {
        try {
            return Priority.valueOf(priority).ordinal();
        } catch (IllegalArgumentException e) {
            return Priority.NORMAL.ordinal();
        }
    }

    private AllocationFailedEvent.FailedLine toFailedLine(OrderReceivedEvent.OrderLine line, int avaibleQuantity,int allocatedQuantity) {
        return AllocationFailedEvent.FailedLine.builder()
			.productId(line.getSku())
			.orderId(line.getOrderId())
			.requestedQuantity(line.getQuantity())
			.availableQuantity(avaibleQuantity)
			.allocatedQuantity(allocatedQuantity)
			.shortageQuantity(avaibleQuantity-allocatedQuantity)
			.build();
    }

    // ─── Publication ─────────────────────────────────────────────────────────────

    private void publishStockAllocated(OrderReceivedEvent event,
                                       List<StockAllocatedEvent.AllocatedLine> lines) {
        kafkaEventPublisher.publishStockAllocated(StockAllocatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .warehouseId(event.getWarehouseId())
                .allocatedLines(lines)
                .occurredAt(Instant.now())
                .build());
        log.info("[ALLOC] stock.allocated — orderId={} lines={}", event.getOrderId(), lines.size());
    }

    private void publishAllocationFailed(OrderReceivedEvent event,
                                         List<AllocationFailedEvent.FailedLine> failed,
                                         AllocationFailedEvent.FailureReason reason) {
        kafkaEventPublisher.publishAllocationFailed(AllocationFailedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .warehouseId(event.getWarehouseId())
                .failureReason(reason)
                .failedLines(failed)
                .retryCount(0)
                .occurredAt(Instant.now())
                .build());
        log.warn("[ALLOC] allocation.failed — orderId={} reason={}", event.getOrderId(), reason);
    }

    // ─── Type interne ─────────────────────────────────────────────────────────────

    private record LineAllocation(Sku sku, int qty) {}
}

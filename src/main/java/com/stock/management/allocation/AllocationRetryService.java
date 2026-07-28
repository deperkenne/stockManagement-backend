package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.allocationLine.AllocationItemStatus;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockReleasedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderNotFoundException;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.domain.*;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.domain.Sku;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Replays orders that could not be fully allocated due to stock shortage.
 *
 * Case 1 — ALLOCATION_FAILED (completeDeliveryRequired=true):
 *   No stock was ever reserved (all-or-nothing gate rejected before any reserve).
 *   Safe to replay the full order.
 *
 * Case 2 — Lines with skuId=null (NOT_ALLOCATED) or remainingQuantity > 0 (WAITING_STOCK):
 *   Only the unallocated portion is replayed, using remainingQuantity as the new quantity.
 *   Stale AllocationItem records are soft-deleted first to prevent duplicates when
 *   AllocationItemService.onStockAllocated() creates fresh records after the new allocation.
 *
 * deleted=false on AllocationItem ensures cancelled lines are never re-queued.
 *
 * Appel direct (sans Kafka) : déclenché par OrderService après une libération de stock.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AllocationRetryService {

    private final OrderRepository orderRepository;
    private final AllocationItemRepository allocationItemRepository;
	private final ApplicationEventPublisher eventPublisher;
	private final SkuRepository skuRepository;
	@PersistenceContext
	private final EntityManager entityManager;

    /**
     * REQUIRES_NEW : point d'entrée public, isolé de la transaction appelante (typiquement
     * OrderService.cancelOrder/cancelLineItems). Rejouer les commandes en attente est un
     * effet secondaire "best effort" : son échec ne doit jamais faire échouer l'annulation
     * qui vient de libérer le stock.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void retryPendingOrders(List<AllocationItem> allocationItems) {
        if (allocationItems == null || allocationItems.isEmpty()) return;

		// recuperation de tous les produitNr
        List<String> productNrs = allocationItems.stream()
            .map(line -> line.getProductNr().getValue())
            .filter(p -> p != null && !p.isBlank())
            .distinct()
            .toList();

        if (productNrs.isEmpty()) return;

        retryFailedOrders(productNrs);
        retryWaitingLines(productNrs);
    }

    // ─── Case 1: ALLOCATION_FAILED ────────────────────────────────────────────────

    private void retryFailedOrders(List<String> productNrs) {



        List<CustomerOrder> orders = orderRepository
            .findByStatusAndLineProductNrs(OrderStatus.ALLOCATION_FAILED, productNrs);

		// B. Verrou pessimiste uniquement sur la commande racine (évite l'exception Hibernate 7)
		entityManager.lock(orders, LockModeType.PESSIMISTIC_WRITE);

        if (orders.isEmpty()) return;

		// publish orders to allocate
        log.info("[RETRY] {} ALLOCATION_FAILED orders eligible for full replay", orders.size());
		// 2. Mapping de la liste de CustomerOrder vers List<OrderReceivedEvent>
			List<OrderReceivedEvent> events = orders.stream()
			.map(this::toOrderReceivedEvent)
			.toList();
		eventPublisher.publishEvent(events);
       // orders.forEach(order -> safeRetry(order.getId().toString(), () -> republishFull(order)));
    }


	// ─── Case 2: WAITING_STOCK / NOT_ALLOCATED (skuId=null) ──────────────────────
	public void retryWaitingLines(List<String> productNrs) {

		// 1. Récupération des AllocationItem en attente
		List<AllocationItem> waitingItems = allocationItemRepository
			.findWaitingItemsByProductNrs(productNrs, AllocationItemStatus.WAITING_STOCK);

		if (waitingItems.isEmpty()) {
			log.info("[RETRY] Aucune ligne en attente de stock pour les produits spécifiés.");
			return;
		}


		// 2. REGROUPEMENT PAR ORDER_ID
		Map<UUID, List<AllocationItem>> itemsByOrderId = waitingItems.stream()
			.collect(Collectors.groupingBy(AllocationItem::getOrderId));

		// 3. Traitement par commande avec transmission des IDs de lignes
		for (Map.Entry<UUID, List<AllocationItem>> entry : itemsByOrderId.entrySet()) {
			UUID orderId = entry.getKey();

			// Extraction des IDs de lignes pour CETTE commande
			List<String> lineIds = entry.getValue().stream()
				.map(line -> line.getProductNr().toString())
				.sorted()
				.toList();

			try {
				// Appel du service transactionnel
				processOrderedAndLockedLinesForOrder(orderId, lineIds,entry.getValue());
			} catch (Exception e) {
				log.error("[RETRY-BATCH] Failed processing order {}", orderId, e);
			}
		}

		log.info("[RETRY-BATCH] Processing {} order(s) impacted by retry", itemsByOrderId.size());

	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void processOrderedAndLockedLinesForOrder(UUID orderId, List<String> productNrs,List<AllocationItem>allocationItems) {
		boolean hasAnyAllocationChanged = false;

		// 1. Charger et verrouiller l'agrégat parent CustomerOrder
		CustomerOrder order = orderRepository.findById(new OrderId(orderId))
			.orElseThrow(() -> new OrderNotFoundException(orderId.toString()));

		entityManager.lock(order, LockModeType.PESSIMISTIC_WRITE);

		// recupere tous les sku et block ses ligne de sorte a ce que les autres transaction ne puisse pas acceder
		// attention cei vas echouer si la donner n'est pas persistente quand on vas redemarer le serveur et les consummer kafka vons
		// redemarer automatiquement
		// si on utilise H2 sa vas planter car la donnee ne serra plus memoire  au moment ou les consummer kafka vont rejouer cette methode
		// Verrouillage pessimiste en BDD
		List<Sku> allSkus = skuRepository.findAvailableSkusForAllocationWithLock(productNrs);

		Map<String, List<Sku>> stockMap = allSkus.stream()
			.collect(Collectors.groupingBy(s -> s.getProductNr().getValue()));


		// Parcours et traitement des lignes
		for ( AllocationItem allocationItemline : allocationItems) {
			entityManager.lock(allocationItemline, LockModeType.PESSIMISTIC_WRITE);
			String productNrStr = allocationItemline.getProductNr().toString();

			int requestedQty = allocationItemline.getRemainingQuantity(); // most important

			//UUID lineIdUuid = UUID.fromString(line.getOrderLineItemId());

			// call greedy allocated
			List<LineAllocation> lineAllocs = Helper.greedyAllocate( allSkus, requestedQty); // greedyAllocated

			int totalAllocated = lineAllocs.stream().mapToInt(LineAllocation::qty).sum();


			if (totalAllocated == 0) { // la ligne n'a pas pu etre allouer alors aucune table Order ou Allocation n'est affecter
				continue;
			}



			// allocation des qty en stock
			for (LineAllocation la : lineAllocs) {
				la.sku().reserve(new Quantity(la.qty()));
			}


			if (totalAllocated < requestedQty) {
				allocationItemline.resetRemainingQty(requestedQty-totalAllocated);
			}else {
				// CAS TOTAL: ligne complètement allouée
				allocationItemline.resetRemainingQty(0);
				allocationItemline.changeStatus(AllocationItemStatus.ALLOCATED);
				allocationItemRepository.save(allocationItemline);

				// Mettre à jour le statut dans la commande managée
				order.getLineItems().stream()
					.filter(line -> line.getId().equals(allocationItemline.getLineItemId()))
					.findFirst()
					.ifPresent(line -> line.changeLineStatus(LineItemStatus.FULLY_ALLOCATED));
				hasAnyAllocationChanged = true;

			}

			// Si rien n'a changé, pas d'update global inutiles
			if (!hasAnyAllocationChanged) {
				return;
			}

			// 5. Condition 1: Vérification si TOUTES les lignes de la commande sont COMPLETE_ALLOCATED
			boolean allLinesAllocated = order.getLineItems().stream()
				.allMatch(line -> line.getStatus() == LineItemStatus.FULLY_ALLOCATED);

			if (allLinesAllocated) {
				order.updateOrderStatus(OrderStatus.FULLY_ALLOCATED);
				log.info("[RETRY-ORDER] Order {} fully allocated -> COMPLETE_DELIVERY", orderId);
			}

		}


	}



	public void retryWaitingLines2(List<String> productNrs) {

		// 1. Récupération des AllocationItem en attente
		List<AllocationItem> waitingItems = allocationItemRepository
			.findWaitingItemsByProductNrs(productNrs, AllocationItemStatus.WAITING_STOCK);

		if (waitingItems.isEmpty()) {
			log.info("[RETRY] Aucune ligne en attente de stock pour les produits spécifiés.");
			return;
		}

		// 2. Regrouper les AllocationItem par OrderId
		Map<UUID, List<AllocationItem>> itemsByOrderId = waitingItems.stream()
			.collect(Collectors.groupingBy(AllocationItem::getOrderId));

		List<OrderReceivedEvent> retryEvents = new ArrayList<>();

		// 3. Générer l'événement partiel pour chaque commande de manière isolée
		for (Map.Entry<UUID, List<AllocationItem>> entry : itemsByOrderId.entrySet()) {
			UUID orderId = entry.getKey();
			List<AllocationItem> orderWaitingItems = entry.getValue();

			try {
				// La sous-méthode retourne un Optional<OrderReceivedEvent>
				processOrderPartialRetry(new OrderId(orderId), orderWaitingItems)
					.ifPresent(retryEvents::add);
			} catch (Exception e) {
				log.error("[RETRY] Échec du traitement de retry pour la commande {}", orderId, e);
			}
		}

		if (retryEvents.isEmpty()) {
			return;
		}

		// 4. Publication de la LISTE complète attendue par eventPublisher
		log.info("[RETRY] Publishing {} partial retry order event(s)", retryEvents.size());
		eventPublisher.publishEvent(retryEvents);
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Optional<OrderReceivedEvent> processOrderPartialRetry(OrderId orderId, List<AllocationItem> waitingItems) {

		CustomerOrder order = orderRepository.findByIdWithLineItems(orderId)
			.orElseThrow(() -> new EntityNotFoundException("Order non trouvée : " + orderId));

		entityManager.lock(order, LockModeType.PESSIMISTIC_WRITE);

		Map<String, Integer> remainingQtyByLineId = waitingItems.stream()
			.collect(Collectors.toMap(
				item -> item.getLineItemId().toString(),
				AllocationItem::getRemainingQuantity
			));

		List<OrderReceivedEvent.OrderLine> retryLines = order.getLineItems().stream()
			.filter(line -> remainingQtyByLineId.containsKey(line.getId().toString()))
			.map(line -> {
				int quantityToRetry = remainingQtyByLineId.get(line.getId().toString());

				return OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(line.getId().toString())
					.orderId(order.getId().toString())
					.sku(line.getProductNr() != null ? line.getProductNr().getValue() : null)
					.status(line.getStatus())
					.quantity(quantityToRetry) // ⚠️ Uniquement la quantité restante
					.unitPrice(line.getUnitPrice())
					.build();
			})
			.toList();

		if (retryLines.isEmpty()) {
			return Optional.empty();
		}

		OrderReceivedEvent retryEvent = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(order.getId().toString())
			.lines(retryLines)
			.currency(order.getCurrency())
			.totalAmount(calculateTotalAmountForLines(retryLines))
			.priority(order.getPriority().toString())
			.completeDeliveryRequired(false)
			.occurredAt(Instant.now())
			.build();

		return Optional.of(retryEvent);
	}

	private void retryWaitingLines1(List<String> productNrs) {

		// retreive all sku == null with status WAITING_STOCK
		List<AllocationItem> waiting = allocationItemRepository
			.findWaitingItemsByProductNrs(productNrs, AllocationItemStatus.WAITING_STOCK);


		if (waiting.isEmpty()) return;

		// 2. Regrouper les AllocationItem par OrderId
		Map<UUID, List<AllocationItem>> itemsByOrderId = waiting.stream()
			.collect(Collectors.groupingBy(AllocationItem::getOrderId));
		log.info("[RETRY] {} commandes impactées par le retry de stock", itemsByOrderId.size());

		List<OrderReceivedEvent> retryEvents = new ArrayList<>();

		// 2. Extraire les identifiants uniques typés (List<OrderId>)
		List<OrderId> orderIds = waiting.stream()
			.map(item -> new OrderId(item.getOrderId())) // Convertit le UUID en ton objet OrderId
			.distinct()
			.toList();

		// 3. Charger les VRAIS objets CustomerOrder complets et managés depuis la DB
		// Cela garantit la présence du champ @Version et évite d'écraser des données
		List<CustomerOrder> orders = orderRepository.findAllById(orderIds);



		// publish orders to allocate
		log.info("[RETRY] {} ALLOCATION_FAILED orders eligible for full replay", orders.size());
		// 2. Mapping de la liste de CustomerOrder vers List<OrderReceivedEvent>
		List<OrderReceivedEvent> events = orders.stream()
			.map(this::toOrderReceivedEvent)
			.toList();

		log.info("[RETRY] Publishing retry event for {} orders", orders.size());

		// 3. Publier directement la liste des vraies commandes
		eventPublisher.publishEvent(events);
	}

	/**
	 * Mappe un objet CustomerOrder (Domaine/Entité) vers le DTO d'événement OrderReceivedEvent.
	 */
	private OrderReceivedEvent toOrderReceivedEvent(CustomerOrder order) {
		List<OrderReceivedEvent.OrderLine> eventLines = order.getLineItems().stream()
			.map(line -> OrderReceivedEvent.OrderLine.builder()
				.orderLineItemId(line.getId() != null ? line.getId().toString() : null)
				.orderId(order.getId().toString())
				.sku(line.getProductNr() != null ? line.getProductNr().getValue() : null)
				.status(line.getStatus())
				.quantity(line.getRequestedQty().getValue())
				.unitPrice(line.getUnitPrice())
				.build())
			.toList();

		return OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(order.getId().toString())
			.lines(eventLines)
			.currency(order.getCurrency())
			.totalAmount(calculateTotalAmount(order))
			.priority(order.getPriority().toString())
			.completeDeliveryRequired(order.isCompleteDeliveryRequired())
			.occurredAt(Instant.now())
			.build();
	}

	// il doivent aller dans le helper
	public BigDecimal calculateTotalAmount(CustomerOrder order) {
		return order.getLineItems().stream()
			.map(li -> li.getUnitPrice().multiply(BigDecimal.valueOf(li.getRequestedQty().getValue())))
			.reduce(BigDecimal.ZERO, BigDecimal::add);
	}

	/**
	 * Calcule le montant total uniquement pour les lignes et quantités rejouées.
	 */
	private BigDecimal calculateTotalAmountForLines(List<OrderReceivedEvent.OrderLine> lines) {
		if (lines == null || lines.isEmpty()) {
			return BigDecimal.ZERO;
		}

		return lines.stream()
			.map(line -> {
				BigDecimal price = line.getUnitPrice() != null ? line.getUnitPrice() : BigDecimal.ZERO;
				BigDecimal quantity = BigDecimal.valueOf(line.getQuantity());
				return price.multiply(quantity);
			})
			.reduce(BigDecimal.ZERO, BigDecimal::add);
	}

	/*
    private void republishFull(CustomerOrder order) {
        List<OrderReceivedEvent.OrderLine> lines = order.getLineItems().stream()
            .map(li -> OrderReceivedEvent.OrderLine.builder()
                .orderLineItemId(li.getId().getValue().toString())
                .orderId(order.getId().toString())
                .sku(li.getProductNr().getValue())
                .quantity(li.getRequestedQty().getValue())
                .unitPrice(li.getUnitPrice())
                .build())
            .toList();

        triggerAllocation(order, lines);
        log.info("[RETRY] Full replay orderId={} lines={}", order.getId(), lines.size());
    }
  */


	/*
    private void republishPartial(UUID rawOrderId, List<AllocationItem> waitingItems) {
        OrderId orderId = new OrderId(rawOrderId);
        CustomerOrder order = orderRepository.findByIdWithLineItems(orderId).orElse(null);
        if (order == null) return;

        // AllocationItem has no unit price — resolve from LineItem for totalAmount calculation
        Map<UUID, LineItem> lineMap = order.getLineItems().stream()
            .collect(Collectors.toMap(li -> li.getId().getValue(), li -> li));

		// create a new order for allocationService

		CustomerOrder order =

        List<OrderReceivedEvent.OrderLine> lines = waitingItems.stream()
            .map(item -> {
                LineItem li = lineMap.get(item.getLineItemId());
                return OrderReceivedEvent.OrderLine.builder()
                    .orderLineItemId(item.getLineItemId().toString())
                    .orderId(rawOrderId.toString())
                    .sku(item.getProductNr().getValue())
                    .quantity(item.getRemainingQuantity())   // only what is still missing
                    .unitPrice(li != null ? li.getUnitPrice() : BigDecimal.ZERO)
                    .build();
            })
            .toList();

        // Soft-delete stale records BEFORE replay — AllocationItemService will create fresh ones
        allocationItemRepository.softDeleteWaitingByOrderId(rawOrderId);

        triggerAllocation(order, lines);
        log.info("[RETRY] Partial replay orderId={} waitingLines={}", rawOrderId, lines.size());
    }
    */
    // ─── Shared helpers ───────────────────────────────────────────────────────────
	/*
    private void triggerAllocation(CustomerOrder order, List<OrderReceivedEvent.OrderLine> lines) {
        BigDecimal total = lines.stream()
            .map(l -> l.getUnitPrice().multiply(BigDecimal.valueOf(l.getQuantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        allocationService.allocate(OrderReceivedEvent.builder()
            .eventId(UUID.randomUUID().toString())
            .orderId(order.getId().toString())
            .customerId(order.getCustomerId())
            .lines(lines)
            .currency(order.getCurrency())
            .totalAmount(total)
            .priority(order.getPriority().name())
            .completeDeliveryRequired(order.isCompleteDeliveryRequired())
            .occurredAt(Instant.now())
            .build());
    }
   */
    private void safeRetry(String orderId, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.error("[RETRY] Failed to re-queue orderId={}: {}", orderId, e.getMessage());
        }
    }
}

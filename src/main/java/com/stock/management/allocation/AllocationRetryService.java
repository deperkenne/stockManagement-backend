package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.allocationLine.AllocationItemStatus;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockReleasedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderNotFoundException;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.*;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.SkuService;
import com.stock.management.sku.domain.Sku;
import com.stock.management.sku.dto.SkuResponse;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final SkuService skuService;
	private final OrderService orderService;
	private final TransactionTemplate transactionTemplate;
	private final OrderRepository orderRepository;
    private final AllocationItemRepository allocationItemRepository;
	private final ApplicationEventPublisher eventPublisher;
	private final SkuRepository skuRepository;
	private final AllocationService allocationService;
	private final AllocationItemService allocationItemService;
	@PersistenceContext
	private final EntityManager entityManager;

	private static final int CHUNK_SIZE = 5;

    /**
     * REQUIRES_NEW : point d'entrée public, isolé de la transaction appelante (typiquement
     * OrderService.cancelOrder/cancelLineItems). Rejouer les commandes en attente est un
     * effet secondaire "best effort" : son échec ne doit jamais faire échouer l'annulation
     * qui vient de libérer le stock.
     */

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void retryPendingOrders(List<AllocationItem> allocationItems) {

		List<CustomerOrder> orders = new ArrayList<>();

        if (allocationItems == null || allocationItems.isEmpty()) return;

		// recuperation de tous les produitNr

		List<String> productNrs = allocationItems.stream()
			.map(item -> item.getProductNr().getValue())
			.filter(p -> p != null && !p.trim().isEmpty())
			.distinct()
			.toList();

        if (productNrs.isEmpty() || productNrs == null) return;




		// 2. Exécution sécurisée dans une NOUVELLE transaction isolée
		//transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

		log.info("[RETRY-SERVICE] Début du rejeu pour {} produit(s).", productNrs.size());
		retryFailedOrders(productNrs,orders);
        // Transaction 1 : Commandes en échec complet
		try {
			retryFailedOrders(productNrs,orders);
		    //	transactionTemplate.executeWithoutResult(status -> retryFailedOrders(productNrs,orders));
		} catch (Exception ex) {
			log.error("[RETRY-SERVICE-ERROR] Échec du rejeu complet : {}", ex.getMessage(), ex);
		}

        // Transaction 2 : Lignes partielles (Totalement indépendante de la Transaction 1)
		try {
			retryLinesForProducts(productNrs,orders);
			//transactionTemplate.executeWithoutResult(status -> retryLinesForProducts(productNrs,orders));
		} catch (Exception ex) {
			log.error("[RETRY-SERVICE-ERROR] Échec du rejeu partiel : {}", ex.getMessage(), ex);
		}
    }

    // ─── Case 1: ALLOCATION_FAILED ────────────────────────────────────────────────

    private void retryFailedOrders(List<String> productNrs , List<CustomerOrder> orders) {

		log.info("\u001B[34m[FINDPRODUCT] getDATA TO REPO\u001B[0m");
		 orders = orderRepository
			.findModifiableOrderWithLineItem(productNrs,List.of(OrderStatus.FULLY_ALLOCATED,OrderStatus.CANCELLED));
		log.info("[list order to replay .............]- orderSize={}",orders.size());
        if (orders.isEmpty()) return;

		List<CustomerOrder> filteredOrdersAllocationFail = orders.stream()
			.filter(o -> o.getStatus() == OrderStatus.ALLOCATION_FAILED)  // adaptez le statut recherché
			.toList();

		log.info("[list order to replay with Failed status]- orderSize={}",filteredOrdersAllocationFail.size());

		// publish orders to allocate
        log.info("[RETRY] {} ALLOCATION_FAILED orders eligible for full replay", orders.size());

		// 2. Mapping de la liste de CustomerOrder vers List<OrderReceivedEvent>
		List<OrderReceivedEvent> events = mapOrderToOrderReceivedEvent(filteredOrdersAllocationFail);

		log.info("\u001B[34m[CALL-ALLOCATION-SERVICE]  {}  start retry process\u001B[0m",events.size());

		List<List<OrderReceivedEvent>> chunks = ListUtils.partition(events, CHUNK_SIZE);

		for (List<OrderReceivedEvent> chunk : chunks) {
			try {

				log.info("[orderToWithNOTALLOCATEDretry]- size={}",
					chunk.size());
				// 🛡️ Transaction isolée pour ce chunk de 10 commandes
				// ici nous avons un batch total en cas de rollback toute les commande s'annule
				// donc probleme les clients vont se plaindres
				// solution chunk  ont decoupe en petit morceau si un lot se pert on continue avec les autres sans du rollback
				allocate(chunk); // ici nous avons un batch total en cas de rollback toute les commande s'annule
				log.info("allocation sucessfull............................");
			} catch (Exception ex) {
				log.error("[REPLAY CHUNK FAILED] Failed to allocate chunk of {} orders. Continuing with next chunk...",
					chunk.size(), ex);
				// Seules les 10 commandes de ce chunk font un rollback
			}
		}

    }


	private List<OrderReceivedEvent> mapOrderToOrderReceivedEvent(List<CustomerOrder>orders){
		return  orders.stream()
				.map(this::toOrderReceivedEvent)
				.toList();
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
				la.sku().reserveQty(new Quantity(la.qty()));
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


	//  **+++++++++++++++++++++++++++++++++++++++++++++

	public void retryLinesForProducts(List<String> productNrs,List<CustomerOrder>orders) {
		List<RetryRecord> linesToReplay = collectReplayLines(productNrs,orders);

		if (linesToReplay.isEmpty()) {
			log.info("[RETRY] Aucune ligne à rejouer pour {}", productNrs);
			return;
		}

		log.info("[collection] lineSize={}, productNr={} qty={}" , linesToReplay.size(),
			linesToReplay.getFirst().productNr(),linesToReplay.getFirst().remainingQuantity());

		Map<UUID, List<RetryRecord>> linesByOrder = linesToReplay.stream()
			.collect(Collectors.groupingBy(RetryRecord::orderId));

		linesByOrder.forEach(this::retryOrderSafely);

		log.info("[RETRY-BATCH] {} commande(s) traitée(s), {} ligne(s) rejouée(s)",
			linesByOrder.size(), linesToReplay.size());
	}

	/**
	 * Union des deux sources de vérité :
	 *  - AllocationItem déjà marqués WAITING_STOCK (retry classique)
	 *  - OrderLineItem NOT_ALLOCATED sans AllocationItem associé (ligne "orpheline",
	 *    jamais persistée lors de la 1ère tentative — cf. bug totalAllocated == 0)
	 * afin de ne perdre aucune ligne, quel que soit l'état historique en base.
	 */
	private List<RetryRecord> collectReplayLines(List<String> productNrs,List<CustomerOrder>orders) {
		List<RetryRecord> waitingAllocations = allocationItemRepository
			.findWaitingItemsByProductNrs(productNrs, AllocationItemStatus.WAITING_STOCK)
			.stream()
			.map(RetryRecord::fromAllocationItem)
			.toList();


		Set<UUID> lineIdsAlreadyCovered = waitingAllocations.stream()
			.map(RetryRecord::lineItemId)
			.collect(Collectors.toSet());

		List<RetryRecord> orphanLines = orderRepository
			.findOrdersHavingLineStatus(productNrs, LineItemStatus.NOT_ALLOCATED)
			.stream()
			.flatMap(order -> extractOrphanLines(order, productNrs, lineIdsAlreadyCovered))
			.toList();

		return Stream.concat(waitingAllocations.stream(), orphanLines.stream()).toList();
	}

	private Stream<RetryRecord> extractOrphanLines(
		CustomerOrder order,
		List<String> productNrs,
		Set<UUID> lineIdsAlreadyCovered) {

		UUID orderId = order.getId().getValue();

		return order.getLineItems().stream()
			.filter(line -> line.getStatus() == LineItemStatus.NOT_ALLOCATED)
			.filter(line -> productNrs.contains(line.getProductNr().getValue()))
			.filter(line -> !lineIdsAlreadyCovered.contains(line.getId()))
			.map(line -> RetryRecord.fromNeverAllocatedLine(orderId, line));
	}

	private void retryOrderSafely(UUID orderId, List<RetryRecord> lines) {
		try {
			replayOrderLinesTransactionally(orderId, lines);
		} catch (Exception e) {
			log.error("[RETRY-BATCH] Échec de traitement pour la commande {}", orderId, e);
		}
	}

	//@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void replayOrderLinesTransactionally(UUID orderId, List<RetryRecord> lines) {
		if(lines.isEmpty()){
			log.info("lines muss not empty");
			return;
		}
		CustomerOrder order = loadAndLockOrder(orderId);
		log.info("[OrderId]- orderId={}",order.getId().getValue());
		Map<String, List<Sku>> stockByProduct = lockAndGroupAvailableStock(lines);

		log.info("[SKusAvailable]- skuSize={}",stockByProduct.size());
        if(stockByProduct.isEmpty()){
			log.info("Skus not available for this products");
			return;
		}
		boolean anyLineFullyAllocated = false; // une aide  a decider si on devrais changer le status le line concerner et sa  commande
		for (RetryRecord line : lines) {
			anyLineFullyAllocated |= replayLine(order, line, stockByProduct);
		}

		// si true allors essayon de changer le status de la commande
		if (anyLineFullyAllocated) {
			promoteOrderStatusIfFullyAllocated(order, orderId);
		}
	}

	private CustomerOrder loadAndLockOrder(UUID orderId) {
		CustomerOrder order = orderRepository.findById(new OrderId(orderId))
			.orElseThrow(() -> new OrderNotFoundException(orderId.toString()));
		entityManager.lock(order, LockModeType.PESSIMISTIC_WRITE);
		return order;
	}

	private Map<String, List<Sku>> lockAndGroupAvailableStock(List<RetryRecord> lines) {
		List<Sku>skus = skuRepository.findAll();
		skus.forEach(s -> log.info("[SKU DEPR]: productNr='{}', qty={} tqty={}",
			s.getProductNr().getValue(), s.getAvailableQuantity(),s.getTotalQuantity()));

		if (skus.isEmpty()){
			log.info("empty sku list");
		}

		List<String> productNrs = lines.stream().map(RetryRecord::productNr).distinct().toList();
		log.warn("[productnrs list]- productnrs={}",productNrs.getFirst());
		if(productNrs.isEmpty()){
			log.warn("productsNrs list is empty");
			return null;
		}
		return skuRepository.findAvailableSkusForAllocationWithLock(productNrs).stream()
			.collect(Collectors.groupingBy(sku -> sku.getProductNr().getValue()));
	}

	/**
	 * Tente d'allouer une ligne (déjà en attente ou jamais allouée),
	 * met à jour (ou crée) son AllocationItem, et propage le changement
	 * de statut sur la ligne de commande si elle devient complète.
	 *
	 * @return true si la ligne vient de passer à FULLY_ALLOCATED
	 */
	private boolean replayLine(CustomerOrder order, RetryRecord line, Map<String, List<Sku>> stockByProduct) {
		List<Sku> availableSkus = stockByProduct.getOrDefault(line.productNr(), List.of());
		List<LineAllocation> allocations = Helper.greedyAllocate(availableSkus, line.remainingQuantity());
		int allocatedQty = allocations.stream().mapToInt(LineAllocation::qty).sum();

		if (allocatedQty == 0) {
			return false; // toujours rien de dispo, on retentera au prochain cycle
		}

		reserveStock(allocations);

		// essai la reallocation en dimuniant la qty
		int stillMissing = line.remainingQuantity() - allocatedQty;
		upsertAllocationItem(line, stillMissing,order,allocations);

		if (stillMissing > 0) {
			return false; // partiellement allouée : reste en attente
		}

		// change specific line status only
		markLineAsFullyAllocated(order, line.lineItemId());
		return true;

	}

	private void reserveStock(List<LineAllocation> allocations) {
		allocations.forEach(a -> a.sku().reserveQty(new Quantity(a.qty())));
	}


	private AllocationItem buildSuccessAllocationItem(UUID orderId, RetryRecord retryRecord,
													  LineAllocation la,int requestedQty) {
		return AllocationItem.builder()
			.orderId(orderId)
			.skuId(UUID.fromString(la.sku().getId().getValue().toString()))
			.lineItemId(UUID.fromString(retryRecord.lineItemId().toString()))
			.productNr(new ProductNr(retryRecord.productNr()))
			.status(AllocationItemStatus.ALLOCATED)
			.quantity(requestedQty)
			.remainingQuantity(0)
			.build();
	}

	private AllocationItem buildPartialAllocationItem(UUID orderId,RetryRecord item, int shortage) {
		return AllocationItem.builder()
			.orderId(orderId)
			.skuId(null)
			.lineItemId(item.lineItemId())
			.productNr(new ProductNr(item.productNr()))
			.status(AllocationItemStatus.WAITING_STOCK)
			.quantity(0)
			.remainingQuantity(shortage)
			.build();
	}


	private void reallocateNotLineAllo(List<LineAllocation>lineAllocations,RetryRecord item,
									   int restQty , CustomerOrder order){

		// Structures de collecte de données unifiées
		List<AllocationItem> allocated = new ArrayList<>();
		Map<UUID, LineItemStatus> lineItemStatusMap = new HashMap<>();

		// La réservation du stock a déjà été faite par replayLine() via reserveStock(lineAllocations)
		// avant l'appel à upsertAllocationItem() -> ne pas réserver une seconde fois ici.
		for (LineAllocation lineAllocation : lineAllocations) {
			allocated.add(buildSuccessAllocationItem(item.orderId(), item, lineAllocation, lineAllocation.qty())); // reserver dans le batch ou liste les ligne allouer
		}

		if (restQty > 0) {

			lineItemStatusMap.put(item.lineItemId(), LineItemStatus.PARTIALLY_ALLOCATED); // reserver le status de la ligne dans un batch

			// reservation du reste de la qty de la ligne qui n'a pas ete totalement allouer
			allocated.add(buildPartialAllocationItem(item.orderId(), item, restQty));
		}
		/**
		 * CAS 3 : ALLOCATION TOTALE
		 */
		else {
			lineItemStatusMap.put(item.lineItemId(), LineItemStatus.FULLY_ALLOCATED);
		}

		notifyStockAllocated(allocated);

		order.updateLineItemStatus(lineItemStatusMap);
		// Pas de promotion du statut de la commande ici : replayOrderLinesTransactionally
		// s'en charge une seule fois (un 2e appel lèverait InvalidOrderStateException)
	}

	private void notifyStockAllocated(List<AllocationItem> allocationItems) {
		if(!allocationItems.isEmpty()) {
			log.info("\u001B[36m[ALLOCSTART]\u001B[0m ALLOCATION {} événement(s)", allocationItems.get(0).getOrderId());

			allocationItemService.onStockAllocated(allocationItems);
			log.info("\u001B[36m[ALLOC] \u001B[0m Allocation succeed—  lines={}", allocationItems.size());
		}
	}



	private void upsertAllocationItem(RetryRecord line,
									  int remainingQty,
									  CustomerOrder order,
									  List<LineAllocation>lineAllocations) {
		AllocationItem item = line.existingAllocationItem();
		if (item != null) {
			// Le stock réservé par ce replay doit être tracé par emplacement (skuId) :
			// sans ces allocations, il ne pourrait jamais être rendu lors d'une annulation
			notifyStockAllocated(lineAllocations.stream()
				.map(la -> buildSuccessAllocationItem(line.orderId(), line, la, la.qty()))
				.toList());

			if (remainingQty == 0) {
				// CAS TOTAL: ligne complètement allouée
				// ici c'est non manager par hibernate donc nous devons save nous meme
				item.resetRemainingQty(0);
				item.changeStatus(AllocationItemStatus.ALLOCATED);
				allocationItemRepository.save(item);
				// item.markAllocated();
			} else {
				item.resetRemainingQty(remainingQty);
			}
			allocationItemRepository.save(item);
		} else {
           reallocateNotLineAllo(lineAllocations,line,remainingQty,order);
		}
	}

	private AllocationItem lockExisting(AllocationItem item) {
		entityManager.lock(item, LockModeType.PESSIMISTIC_WRITE);
		return item;
	}

	private void markLineAsFullyAllocated(CustomerOrder order, UUID lineItemId) {
		order.getLineItems().stream()
			.filter(line -> line.getId().getValue().equals(lineItemId))
			.findFirst()
			.ifPresent(line -> line.changeLineStatus(LineItemStatus.FULLY_ALLOCATED));
	}

	private void promoteOrderStatusIfFullyAllocated(CustomerOrder order, UUID orderId) {
		boolean allLinesAllocated = order.getLineItems().stream()
			.allMatch(line -> line.getStatus() == LineItemStatus.FULLY_ALLOCATED);

		if (allLinesAllocated) {
			order.updateOrderStatus(OrderStatus.FULLY_ALLOCATED);
			log.info("[RETRY-ORDER] Commande {} entièrement allouée", orderId);
		}
	}




	private void allocate(List<OrderReceivedEvent> events) {

		log.info("start order allocation- aalo={}, name={}, size={}", events.get(0).getOrderId(),events.getFirst().getLines().getFirst().getSku().toString(),events.size());

		if(events.isEmpty()){
			log.warn("aucun order diponible................ ");
			return;
		}

		//1. Verrouillage & chargement du stock (Délégué)
		Map<String, List<Sku>> stockMap = skuService.lockAndFetchAvailableStock(events);
		log.warn("[SKU verfügbar ]- skusSize={}", stockMap.size());
		if (stockMap.isEmpty()){
			log.warn("aucun sku disponible................ ");
			return;
		}

		log.info("[ALLOC SKU size]- sku={}",stockMap.size());
		List<UUID> batchOrderUuidIds = events.stream()
			.map(event -> UUID.fromString(event.getOrderId())) // 👈 Conversion String -> UUID
			.toList();
		//Set<UUID> alreadyAllocatedIds = new HashSet<>(
		//	allocationItemService.findAlreadyAllocatedOrderIds(batchOrderUuidIds)
		//);

		// Filtrage en RAM (nanosecondes) : conserve uniquement les nouvelles commandes
		//List<OrderReceivedEvent> filtedEventsToProcess = OrderReceivedEvent.newEventsToProcess(events,alreadyAllocatedIds);

		//  Ordonnancement des commandes selon les règles métier (Délégué)
		List<OrderReceivedEvent> prioritizedEvents = OrderReceivedEvent.sortByPriority(events);

		for (OrderReceivedEvent event : prioritizedEvents) {
			log.info("[CHECHLINEIDIDIDIDDDDDDDDDDDDDDDDDDDDD]Ordre JUSTE APRÈS readValue (outbox): {}",
				event.getLines().stream()
					.map(OrderReceivedEvent.OrderLine::getSku)
					.toList());

			processOrder(event, stockMap);
		}

		log.info("fin...................................................................................................");
	}


	// ─── Logique d'allocation (partagée batch / unitaire) ─────────────────────────

	private void processOrder(OrderReceivedEvent event, Map<String, List<Sku>> stockMap) {

		log.info("start order process", event.getOrderId());

		OrderId orderIdObj = orderService.createOrdrId(UUID.fromString(event.getOrderId()));
		int totalLines = event.getLines().size();

		/**
		 * S de SOLID : La validation "Tout ou Rien" est isolée
		 */

		if (event.isCompleteDeliveryRequired() && !isCompleteDeliveryPossible(event, stockMap)) {
			log.info("change status complete delevryry equals true");
			orderService.handleCompleteDeliveryFailure(orderIdObj);
			return;
		}

		// Structures de collecte de données unifiées
		List<AllocationItem> allocated = new ArrayList<>();
		Map<UUID, LineItemStatus> lineItemStatusMap = new HashMap<>();

		int completelyFailedLines = 0;
		int partialLines = 0;

		// Parcours et traitement des lignes
		// 1. On extrait et on trie les lignes par orderLineItemId avant de traiter

		//List<OrderReceivedEvent.OrderLine> lineItems = event.getLines();
		for (OrderReceivedEvent.OrderLine line : event.getLines()) {
			log.info("[Info SUR la Ligne a traiter] - qty={}",line.getQuantity());
			String productNrStr = line.getSku();
			int requestedQty = line.getQuantity();
			UUID lineIdUuid = UUID.fromString(line.getOrderLineItemId());

			List<Sku> skus = stockMap.getOrDefault(productNrStr, List.of()); // recupere tous les sku destiner a cette ligne

			if (isLineToSkip(line)) {
				System.out.println("error: change status when it always with canceled or full-allocated not possible!!!");
				continue; // Saut immédiat à l'élément suivant (0 coût CPU !)
			}

			List<LineAllocation> lineAllocs = Helper.greedyAllocate(skus, requestedQty); // greedyAllocated
			int totalAllocated = lineAllocs.stream().mapToInt(LineAllocation::qty).sum();

			/**
			 * CAS 1 : AUCUN STOCK ALLOUÉ
			 * si le totalAllocated == 0 alors la ligne n'a pas etet allouer
			 */
			if (lineAllocs.isEmpty() || totalAllocated == 0) {
				log.warn("aucune allocation dispo");
				completelyFailedLines++; // ceci vas nous aider a voir see ont peut passer orderstatus a FAILLED si completelyFailedLines = lines.size()
				lineItemStatusMap.put(lineIdUuid, LineItemStatus.NOT_ALLOCATED); // reservation de la  ligne qui n'a pas ete allouer dans un bacht ou list pour changer leur status
				//allocated.add(buildFailedAllocationItem(event, line, requestedQty));
				continue;
			}

			/**
			 * CAS GÉNÉRAL : Réservez le stock trouvé (DRY appliqué)
			 * reserver dans le batch ou list les lignes allouer pour les stocker dans la table allocationItem une seule fois et
			 * eviter les surcharge reseau et surcharge de connexion   (gain et optimatisation car on regroupe et on alloue)
			 */
			for (LineAllocation lineAllocation : lineAllocs) {
				lineAllocation.sku().reserveQty(new Quantity(lineAllocation.qty()));
				log.warn("[ALLOC] Allocation —  allocated={}  orderId={} orderPrio={}" ,
					lineAllocation.qty(),event.getOrderId(),event.getPriority());
				allocated.add(buildSuccessAllocationItem(event, line, lineAllocation, lineAllocation.qty())); // reserver dans le batch ou liste les ligne allouer
			}

			/**
			 *
			 * CAS 2 : ALLOCATION PARTIELLE
			 * resrever le status de la ligne  qui n'a pas ete totalement allouer  pour la modifier dans la table order
			 * verification s'il y'a eu reste qui n'a pas ete allouer  ensuite le stocker le reste dans le batch ou list
			 */
			if (totalAllocated < requestedQty) {
				log.warn("[ALLOC] Partial — orderId={} sku={} needed={} got={} orderPriority={}",
					event.getOrderId(), line.getOrderLineItemId(), requestedQty, totalAllocated,event.getPriority());

				partialLines++; // important car si partial passe au moins a 1 alors l'order aurras le status Partial

				lineItemStatusMap.put(lineIdUuid, LineItemStatus.PARTIALLY_ALLOCATED); // reserver le status de la ligne dans un batch

				// reservation du reste de la qty de la ligne qui n'a pas ete totalement allouer
				allocated.add(buildPartialAllocationItem(event, line, requestedQty - totalAllocated));
			}
			/**
			 * CAS 3 : ALLOCATION TOTALE
			 */
			else {
				lineItemStatusMap.put(lineIdUuid, LineItemStatus.FULLY_ALLOCATED);
			}
		}

		/**
		 * DRY / SOLID : Détermination et mise à jour du statut global
		 */
		OrderStatus finalStatus = determineGlobalOrderStatus(totalLines, completelyFailedLines, partialLines);
		orderService.changeOrderStatus(orderIdObj, finalStatus);
		/**
		 * S de SOLID : Mise à jour des statuts des lignes via ta méthode optimisée Map<UUID, Status>
		 */
		orderService.changeLineStatus(orderIdObj, lineItemStatusMap);
		notifyStockAllocated(allocated); // alloue toutes les ligne qui on ete completement allouer et celle qui ont eu un reste non allouer
	}


	private boolean isCompleteDeliveryPossible(OrderReceivedEvent event , Map<String, List<Sku>> stockMap) {
		return event.getLines().stream()
			.allMatch(l -> totalAvailable(stockMap.get(l.getSku())) >= l.getQuantity());
	}


	private boolean isLineToSkip(OrderReceivedEvent.OrderLine line){
		return !(line.getStatus().equals(LineItemStatus.PENDING) || line.getStatus().equals(LineItemStatus.NOT_ALLOCATED));
	}

	private AllocationItem buildFailedAllocationItem(CustomerOrder order, LineItem line, int requestedQty) {
		return AllocationItem.builder()
			.orderId(UUID.fromString(order.getId().getValue().toString()))
			.skuId(null)
			.lineItemId(UUID.fromString(line.getId().getValue().toString()))
			.productNr(line.getProductNr())
			.status(AllocationItemStatus.NOT_ALLOCATED)
			.quantity(0)
			.remainingQuantity(requestedQty)
			.build();
	}

	private AllocationItem buildSuccessAllocationItem(OrderReceivedEvent orderReceivedEvent,OrderReceivedEvent.OrderLine line, LineAllocation la, int requestedQty) {
		return AllocationItem.builder()
			.orderId(UUID.fromString(orderReceivedEvent.getOrderId()))
			.skuId(UUID.fromString(la.sku().getId().getValue().toString()))
			.lineItemId(UUID.fromString(line.getOrderLineItemId()))
			.productNr(new ProductNr(line.getSku()))
			.status(AllocationItemStatus.ALLOCATED)
			.quantity(requestedQty)
			.remainingQuantity(0)
			.build();
	}

	private AllocationItem buildPartialAllocationItem(OrderReceivedEvent orderReceivedEvent,OrderReceivedEvent.OrderLine line, int shortage) {
		return AllocationItem.builder()
			.orderId(UUID.fromString(orderReceivedEvent.getOrderId()))
			.skuId(null)
			.lineItemId(UUID.fromString(line.getOrderLineItemId()))
			.productNr(new ProductNr(line.getSku()))
			.status(AllocationItemStatus.WAITING_STOCK)
			.quantity(0)
			.remainingQuantity(shortage)
			.build();
	}

	private OrderStatus determineGlobalOrderStatus(int totalLines, int completelyFailedLines, int partialLines) {
		if (completelyFailedLines == totalLines) {
			return OrderStatus.ALLOCATION_FAILED;
		} else if (completelyFailedLines > 0 || partialLines > 0) {
			return OrderStatus.PARTIALLY_ALLOCATED;
		} else {
			return OrderStatus.FULLY_ALLOCATED;
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






	private void orderChangeStatus(List<OrderReceivedEvent> events, OrderStatus orderStatus){
		List<OrderId> domainOrderIds = events.stream()
			.map(OrderReceivedEvent::getOrderId)
			.map(UUID::fromString)
			.map(OrderId::new)
			.collect(Collectors.toList());
		orderRepository.updateStatusForIds(domainOrderIds,orderStatus);
	}




}

package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.allocationLine.AllocationItemStatus;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockAllocatedEvent;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.*;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.SkuService;
import com.stock.management.sku.domain.Sku;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.stock.management.allocationLine.AllocationItemStatus.ALLOCATED;
import static com.stock.management.allocationLine.AllocationItemStatus.NOT_ALLOCATED;

/**
 * Appels directs (sans Kafka) : receiveOrder() → allocate() → onStockAllocated()/handleCompleteDeliveryFailure().
 * OrderService dépend normalement de AllocationService (déclenche l'allocation à la création) ; pour éviter le
 * cycle de beans (AllocationService a besoin d'OrderService pour signaler un échec de livraison complète),
 * la référence retour est chargée en @Lazy — seul un des deux sens du cycle a besoin de l'être.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AllocationService {
    private final OrderRepository orderRepository;
   // private final SkuRepository skuRepository;
    private final AllocationItemService allocationItemService; // avoid to depend on implementation detail
    @Lazy
	private final OrderService orderService; // avoid to depend on implementation detail
	@Lazy
	private final SkuService skuService; // avoid to depend on implementation detail



	/**
	 * REQUIRES_NEW : appelée en appel direct depuis OrderService.receiveOrder() (et depuis
	 * AllocationRetryService). La commande doit rester durablement créée même si l'allocation
	 * échoue ensuite — c'est la même garantie de résilience que l'ancien flux Kafka découplé
	 * (créer la commande et l'allouer étaient deux transactions indépendantes).
	 */
	@Transactional
	public void allocate(List<OrderReceivedEvent> events) {

		log.info("start order allocation- aalo={}, name={}", events.get(0).getOrderId(),events.getFirst().getLines().getFirst().getSku().toString());

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
		Set<UUID> alreadyAllocatedIds = new HashSet<>(
			allocationItemService.findAlreadyAllocatedOrderIds(batchOrderUuidIds)
		);

		// Filtrage en RAM (nanosecondes) : conserve uniquement les nouvelles commandes
		List<OrderReceivedEvent> filtedEventsToProcess = OrderReceivedEvent.newEventsToProcess(events,alreadyAllocatedIds);

		//  Ordonnancement des commandes selon les règles métier (Délégué)
		List<OrderReceivedEvent> prioritizedEvents = OrderReceivedEvent.sortByPriority(events);
		for (OrderReceivedEvent event : prioritizedEvents) {
			log.info("order-information - orderPriority={} ", event.getPriority());
			processOrder(event, stockMap);
		}
	}


	/**
	 * Traitement unitaire : Ouvre une NOUVELLE transaction indépendante pour chaque commande.
	 * Si cette méthode plante et fait un rollback, elle n'impacte PAS les commandes précédentes déjà committées.
	 */
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void allocateSingleOrder(OrderReceivedEvent event){

		Map<String, List<Sku>> stockMap = skuService.fetchAvailableStock(event);
		processOrder(event,stockMap);


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

	public void processOrder(OrderReceivedEvent event, Map<String, List<Sku>> stockMap) {

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

		for (OrderReceivedEvent.OrderLine line : event.getLines()) {
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




   // ─── Méthodes privées d'extraction (SOLID / DRY / No Side Effects) ───────────────
	// ici si on se rend compte que la quantite d'une ligne de commande est superieur a la quantite total en stock
	// on renvoi false   et on arrete le parcours

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


    // ─── Notification directe (remplace l'ancienne publication Kafka) ─────────────

    private void notifyStockAllocated(List<AllocationItem> allocationItems) {
		if(!allocationItems.isEmpty()) {
			log.info("\u001B[36m[ALLOCSTART]\u001B[0m ALLOCATION {} événement(s)", allocationItems.get(0).getOrderId());

			allocationItemService.onStockAllocated(allocationItems);
			log.info("\u001B[36m[ALLOC] \u001B[0m Allocation succeed—  lines={}", allocationItems.size());
		}
    }

    // ─── Type interne ─────────────────────────────────────────────────────────────

   // private record LineAllocation(Sku sku, int qty) {}
}

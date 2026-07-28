package com.stock.management.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stock.management.allocation.AllocationRetryService;
import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockReleasedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.*;
import com.stock.management.order.internal.OrderValidator;
import com.stock.management.sku.SkuService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static com.stock.management.order.domain.OrderStatus.CANCELLED;
import static com.stock.management.order.domain.OrderStatus.PARTIALLY_ALLOCATED;


/**
 * Appels directs (sans Kafka) : receiveOrder() déclenche AllocationService.allocate() immédiatement ;
 * cancelOrder()/cancelLineItems() déclenchent SkuService.releaseBulkStock() + AllocationRetryService
 * directement. AllocationService dépend d'OrderService en retour (@Lazy côté AllocationService évite
 * le cycle de beans) — voir AllocationService pour le détail.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {
    private final AllocationItemService allocationItemService;
    private final OrderRepository orderRepository;
	private final OrderStatusHistoryRepository orderStatusHistoryRepository;
	private final SkuService skuService;
	private final AllocationRetryService allocationRetryService;
    private final OrderValidator orderValidator;
	private final ApplicationEventPublisher eventPublisher;
	private final KafkaEventPublisher kafkaEventPublisher;
	private final ObjectMapper objectMapper;
	private final OrderOutboxRepository outboxRepository;
	// ◄── Spring injecte automatiquement l'EntityManager ici
	@PersistenceContext
	private final EntityManager entityManager;// ◄── Spring injecte automatiquement l'EntityManager ici

    @Transactional
    public boolean orderChangeStatus(List<OrderId> orderIds, OrderStatus newStatus) {
        if (orderIds == null || orderIds.isEmpty()) {
            return false;
        }
        List<OrderId> distinctIds = orderIds.stream().distinct().toList();
        int rowsUpdated = orderRepository.updateStatusForIds(distinctIds, newStatus);
        log.info("[BATCH STATUS] Updated {} orders to status {}", rowsUpdated, newStatus);
        return rowsUpdated == orderIds.size();
    }

    // ─── Création ─────────────────────────────────────────────────────────────────


	@Transactional
	public CreateOrderResponse receiveOrder(CreateOrderRequest request) {
		// 1. Validation de la requête
		orderValidator.validate(request);

		// 2. Création et persistance (Order + Historique initial)
		CustomerOrder order = createAndPersistOrder(request);

		// 3. Publication de l'événement
		//publishOrderReceivedEvent(order);
		createAndPersistOrderOutBox(order);

		// 4. Construction du DTO de réponse
		return orderMapper(order);
	}

    public OrderId createOrdrId(UUID rawUuid){
		return CustomerOrder.createIdFrom(rawUuid);
	}

	public void changeLineStatus(OrderId orderId,Map<UUID,LineItemStatus> linesToUpdateByStatus){
		// Hibernate NE FAIT PAS de requête SQL 'SELECT' ici !
		// Il retrouve l'objet directement en mémoire dans son Persistence Context (1st Level Cache)
		CustomerOrder order = getOrder(orderId);
		order.updateLineItemStatus(linesToUpdateByStatus);
	}

	public void changeOrderStatus(OrderId orderId, OrderStatus orderStatus){
		CustomerOrder order = getOrder(orderId);
		order.updateOrderStatus(orderStatus);
	}

	private CustomerOrder getOrder(OrderId orderId){
		return orderRepository.findById(orderId)
			.orElseThrow(() -> new OrderNotFoundException(orderId.toString()));
	}

    // ─── Lecture (CRUD) ──────────────────────────────────────────────────────────

    /** GET /api/orders — liste légère, sans les lignes (évite de charger toutes les collections LAZY). */
    public List<CreateOrderResponse> findAll() {
        return orderRepository.findAll().stream()
                .map(o -> toOrderResponse(o, false))
                .toList();
    }

    /** Variante paginée — à utiliser dès que le volume de commandes rend findAll() sans pagination coûteux. */
    public Page<CreateOrderResponse> findAll(Pageable pageable) {
        return orderRepository.findAll(pageable).map(o -> toOrderResponse(o, false));
    }

    /** GET /api/orders/{orderId} — détail complet avec toutes les lignes. */
    public CreateOrderResponse findById(UUID orderId) {
        CustomerOrder order = orderRepository.findByIdWithLineItems(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        return toOrderResponse(order, true);
    }

    // ─── Mise à jour (CRUD) ───────────────────────────────────────────────────────

    /** PUT /api/orders/{orderId} — modifie priority / completeDeliveryRequired / currency uniquement. */
    @Transactional
    public CreateOrderResponse updateOrder(UUID orderId, UpdateOrderRequest request) {
        CustomerOrder order = orderRepository.findById(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.updateDetails(request.priority(), request.completeDeliveryRequired(), request.currency());
        log.info("[ORDER] Updated orderId={} priority={} currency={}", orderId, request.priority(), request.currency());
        return toOrderResponse(order, false);
    }

    // ─── Suppression (CRUD) ───────────────────────────────────────────────────────

    /** DELETE /api/orders/{orderId} — supprime la commande. cascade=ALL + orphanRemoval=true supprime aussi ses lignes. */
    @Transactional
    public void deleteOrder(UUID orderId) {
        CustomerOrder order = orderRepository.findById(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        orderRepository.delete(order);
        log.info("[ORDER] Deleted orderId={}", orderId);
    }

    // ─── Lignes de commande — CRUD sur la sous-ressource /lines ──────────────────

    /** POST /api/orders/{orderId}/lines — ajoute une ligne à une commande existante. */
    @Transactional
    public CreateOrderResponse addLineItem(UUID orderId, LineItemRequest request) {
        CustomerOrder order = orderRepository.findByIdWithLineItems(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.addLineItem(new ProductNr(request.productNr()), new Quantity(request.requestedQty()), request.unitPrice());
        log.info("[ORDER] Added line productNr={} to orderId={}", request.productNr(), orderId);
        return toOrderResponse(order, true);
    }

    /** PUT /api/orders/{orderId}/lines/{lineItemId} — modifie quantité/prix d'une ligne existante. */
    @Transactional
    public CreateOrderResponse updateLineItem(UUID orderId, UUID lineItemId, LineItemRequest request) {
        CustomerOrder order = orderRepository.findByIdWithLineItems(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.updateLineItem(new LineItemId(lineItemId), new Quantity(request.requestedQty()), request.unitPrice());
        log.info("[ORDER] Updated lineItemId={} on orderId={}", lineItemId, orderId);
        return toOrderResponse(order, true);
    }

    /** DELETE /api/orders/{orderId}/lines/{lineItemId} — retire une seule ligne (orphanRemoval déclenche le DELETE SQL). */
    @Transactional
    public CreateOrderResponse removeLineItem(UUID orderId, UUID lineItemId) {
        CustomerOrder order = orderRepository.findByIdWithLineItems(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        order.removeLineItem(new LineItemId(lineItemId));
        log.info("[ORDER] Removed lineItemId={} from orderId={}", lineItemId, orderId);
        return toOrderResponse(order, true);
    }

    // ─── Mapping entité → DTO — TOUJOURS fait à l'intérieur de la transaction ────
    // (open-in-view=false : mapper après le retour de la méthode ferait planter tout accès
    // à une collection LAZY comme lineItems avec une LazyInitializationException)

    private CreateOrderResponse toOrderResponse(CustomerOrder order, boolean withLines) {
        List<LineItemResponse> lines = withLines
                ? order.getLineItems().stream().map(this::toLineItemResponse).toList()
                : List.of();
        return new CreateOrderResponse(
			order.getId().toString(),
			order.getStatus().name(),
			order.getLineItems().size(),
			order.getReceivedAt()
		);
    }

    private LineItemResponse toLineItemResponse(LineItem li) {
        return new LineItemResponse(
                li.getId().getValue().toString(),
                li.getProductNr().getValue(),
                li.getRequestedQty().getValue(),
                li.getAllocatedQty().getValue(),
                li.getUnitPrice(),
                li.getStatus().name()
        );
    }

    // ─── Annulation commande complète ─────────────────────────────────────────────

	/**
	 * Annule une commande entière. Idempotent : si elle est déjà CANCELLED, on renvoie l'état
	 * existant sans rien réécrire (évite d'écraser cancelledAt/cancellationSource d'origine).
	 */
	@Transactional
	public CancelOrderResponse cancelOrder(OrderId orderId, CancelOrderRequest request) {
		CustomerOrder order = orderRepository.findByIdWithLineItemsForUpdate(orderId)
			.orElseThrow(() -> new EntityNotFoundException("Order not found: " + orderId));

		// 2. Verrouillage PESSIMISTE propre via l'EntityManager (uniquement sur la racine)
		entityManager.lock(order, LockModeType.PESSIMISTIC_WRITE);

		OrderStatus oldStatus = order.getStatus();

		// verify si l'annulation doit avoir lieu ou pas
		validateCancellationEligibility(oldStatus,UUID.fromString(order.getId().getValue().toString()));

		if (oldStatus == OrderStatus.PARTIALLY_ALLOCATED) {
			log.info("[CANCEL] Order {} is PARTIAL. Initiating stock release...", order.getId());
			releaseStockForPartialOrder(order);
		}


		order.cancel(request.cancellationSource(),request.reason(),request.cancelledBy()); // call cancel function to change all orderLineItemStatus to Cancelled also orderStatus to Cancelled
		orderRepository.save(order);
		orderStatusHistoryRepository.save(OrderStatusHistory.of(
				orderId.getValue(), oldStatus, CANCELLED, "CANCEL:" + request.cancellationSource().name()));

		log.info("[CANCEL] Order {} successfully transitioned from {} to CANCELLED.", orderId, oldStatus);
		return new CancelOrderResponse(
			orderId.toString(),
			CANCELLED.name(),
			0,
			request.cancellationSource().name(),
			order.getCancelledAt()
		);
	}


	@Transactional
	public CancelOrderResponse cancelLineItems(UUID orderId, CancelOrderRequest request) {
		log.info("[CANCEL] Initiating batch cancellation for orderId={}, lines={}", orderId, request.lineItemIds().size());

		// 1. Lock pessimiste + lignes chargées en un seul aller-retour (évite le lock puis lazy-load séparé)
		CustomerOrder order = orderRepository.findByIdWithLineItemsForUpdate(new OrderId(orderId))
			.orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

		OrderStatus oldStatus = order.getStatus();

		// 2. Guard Clauses globales
		validateOrderStateForCancellation(orderId, oldStatus);


		// LE MÉTIER EST ENTIÈREMENT DÉLÉGUÉ À L'ENTITÉ ICI :
		List<LineItemId> targets = order.extractEligibleLineIdsForCancellation(request.lineItemIds());
		if (targets.isEmpty()) {
			log.info("[CANCEL] All requested lines are already cancelled for orderId={}", orderId);
			return new CancelOrderResponse(orderId.toString(), order.getStatus().name(), 0, request.cancellationSource().name(), order.getCancelledAt());
		}

		order.cancelLines(targets); //Change specific  orderLines status to Cancelled


		// Récupération et Soft-Delete des allocations associées pour éviter définitivement le rejeu
		List<UUID> targetUuids = targets.stream().map(LineItemId::getValue).toList();
		List<AllocationItem> allocationsToRelease = findAllAllocationItem(targetUuids);


		if (!allocationsToRelease.isEmpty()) {
			// change allocationItemStatus  to cancel of specific allocationItem
			allocationItemService.updateStatusToCancelledByOrderId(order.getId().getValue());
		}

		// 6. Recalcul et ajustement du statut global de la commande  si le status est partial
		order.evaluateAndModifyGlobalStatus();
		orderRepository.save(order);
		logStatusChangeIfAny(orderId, oldStatus, order.getStatus(), "PARTIAL_CANCEL");

		if (oldStatus == PARTIALLY_ALLOCATED) {
			releaseStockAndRetryPending(order, allocationsToRelease);
		}
		return createCancelResponse(order, allocationsToRelease.size(), request.cancellationSource().name(), Instant.now());
	}


	@Transactional
	public void handleCompleteDeliveryFailure(OrderId orderId) {
		// 1. On récupère UNIQUEMENT le statut actuel via une projection (SELECT très rapide sur index)

		log.error("[FIND order] Order {} for this id ", orderId);
		Optional<OrderStatus> currentStatusOpt = orderRepository.findStatusById(orderId);

		// Cas A : La commande n'existe pas -> On traite l'anomalie
		if (currentStatusOpt.isEmpty()) {
			log.error("[CRITICAL] Order {} does not exist. Cannot process delivery failure.", orderId);
			throw new EntityNotFoundException("Order not found: " + orderId);
		}

		OrderStatus currentStatus = currentStatusOpt.get();

		// Cas B : La commande est DÉJÀ dans le bon statut -> Idempotence, on stoppe proprement
		if (currentStatus == OrderStatus.ALLOCATION_FAILED) {
			log.info("[ORDER-BACKEND] Order {} is already in ALLOCATION_FAILED status. Skipping redundant updates.", orderId);
			return;
		}

		// Cas C : La commande existe et doit être mise à jour
		orderRepository.updateOrderStatus(orderId, OrderStatus.ALLOCATION_FAILED);
		int updatedLines = orderRepository.updateAllLinesStatusIfChanged(orderId, LineItemStatus.NOT_ALLOCATED);
		orderStatusHistoryRepository.save(
				OrderStatusHistory.of(orderId.getValue(), currentStatus, OrderStatus.ALLOCATION_FAILED, "ALLOCATION_FAILED"));

		log.info("[ORDER-BACKEND] Successfully transitioned order {} to ALLOCATION_FAILED. {} lines updated.", orderId, updatedLines);
	}


    // ─── Private helpers ──────────────────────────────────────────────────────────


	private CancelOrderResponse createCancelResponse(CustomerOrder order, int releasedCount, String source, Instant timestamp) {
		return new CancelOrderResponse(
			order.getId().getValue().toString(),
			order.getStatus().name(),
			releasedCount,
			source,
			timestamp
		);
	}

	private void validateOrderStateForCancellation(UUID orderId, OrderStatus status) {
		if (status == OrderStatus.CANCELLED) {
			throw new OrderCancellationException("Cannot modify a fully cancelled order: " + orderId);
		}
		if (status == OrderStatus.FULLY_ALLOCATED) {
			throw new OrderCancellationException("Cannot cancel items on a fully completed order: " + orderId);
		}
	}

	private void validateCancellationEligibility(OrderStatus status, UUID orderId) {
		if (status == OrderStatus.FULLY_ALLOCATED || status == CANCELLED) {
			throw new OrderCancellationException("Cannot cancel a completed or cancelled order: " + orderId);
		}
	}

	/** Enregistre une entrée d'historique uniquement si le statut a réellement changé. */
	private void logStatusChangeIfAny(UUID orderId, OrderStatus oldStatus, OrderStatus newStatus, String reason) {
		if (oldStatus != newStatus) {
			orderStatusHistoryRepository.save(OrderStatusHistory.of(orderId, oldStatus, newStatus, reason));
		}
	}

	// changement de status des allocationitems  a cancelled pour eviter de les rejouer
	private void releaseStockForPartialOrder(CustomerOrder order) {
		// Extraction des IDs de toutes les lignes de la commande
		// 🔍 Récupération directe des UUIDs de toutes les lignes de la commande
		List<UUID> lineItemUuids = order.getLineItems().stream()
			.map(LineItem::getId)
			.map(LineItemId::getValue)
			.toList();

		// stock dans le cahe de hibernette   donc toute les modification se ferront maintenenat dans le cache plus rapide
		// allocationItem to reallocate to SKU (stock)
		List<AllocationItem> activeAllocations = findAllAllocationItem(lineItemUuids);

		if (activeAllocations.isEmpty()) {
			log.info("[CANCEL] No active stock allocations found for partial order {}.", order.getId());
			return;
		}

		// reallocation du stock  c'est dire on remet les quantite qui avaient ete allouer dans allocationItem
		// dans la table sku
		releaseStockAndRetryPending(order, activeAllocations); // qty to reallocate to the specific SKUs

		// change allocationItemStatus  to cancel of specific allocationItem
		allocationItemService.updateStatusToCancelledByOrderId(order.getId().getValue());


	}


	//Votre méthode d'origine devient un orchestrateur très fluide et facile à lire
	private void publishOrderReceivedEvent(CustomerOrder order) {
		OrderReceivedEvent event = buildOrderReceivedEvent(order);
		kafkaEventPublisher.publishOrderReceived(event);
		log.debug("[ORDER] Published order.received for orderId={}", order.getId());
	}

	public BigDecimal calculateTotalAmount(CustomerOrder order) {
		return order.getLineItems().stream()
			.map(li -> li.getUnitPrice().multiply(BigDecimal.valueOf(li.getRequestedQty().getValue())))
			.reduce(BigDecimal.ZERO, BigDecimal::add);
	}

    private OrderReceivedEvent buildOrderReceivedEvent(CustomerOrder order){

		return OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(order.getId().toString())
			.lines(fillOrderLines(order))
			.currency(order.getCurrency())
			.totalAmount(calculateTotalAmount(order))
			.priority(order.getPriority().name())
			.completeDeliveryRequired(order.isCompleteDeliveryRequired())
			.occurredAt(Instant.now())
			.build();
	}


	private List<OrderReceivedEvent.OrderLine> fillOrderLines (CustomerOrder order){
		List<OrderReceivedEvent.OrderLine> lines = order.getLineItems().stream()
			.map(this::toOrderLine)
			.toList();
		  return lines;
	}

	/*
	private void publishOrderReceivedEvent(CustomerOrder order) {

		BigDecimal total = order.getLineItems().stream()
			.map(li -> li.getUnitPrice().multiply(BigDecimal.valueOf(li.getRequestedQty().getValue())))
			.reduce(BigDecimal.ZERO, BigDecimal::add);

		kafkaEventPublisher.publishOrderReceived(order);

		log.debug("[ORDER] Published order.received for orderId={}", order.getId());
	}

	 */



	private CustomerOrder createAndPersistOrder(CreateOrderRequest request) {
		CustomerOrder order = CustomerOrder.create(
			request.priority(),
			request.completeDeliveryRequired(),
			request.currency(),
			request.lineItems()
		);

		log.info("[ORDER] Saving new orderId={} with initial status={}", order.getId(), order.getStatus());

		CustomerOrder savedOrder = orderRepository.save(order);

		orderStatusHistoryRepository.save(
			OrderStatusHistory.of(savedOrder.getId().getValue(), null, savedOrder.getStatus(), "ORDER_RECEIVED")
		);

		return savedOrder;
	}

	private void createAndPersistOrderOutBox(CustomerOrder order){
		OrderReceivedEvent orderReceivedEvent = buildOrderReceivedEvent(order);
		try {
			OrderOutBox outboxEntry = OrderOutBox.builder()
				.id(UUID.randomUUID())
				.aggregateType("ORDER")
				.aggregateId(orderReceivedEvent.getOrderId().toString())
				.eventType("OrderReceivedEvent")
				.payload(objectMapper.writeValueAsString(orderReceivedEvent))
				.status(OrderOutBox.OutboxStatus.PENDING)
				.retryCount(0)
				.createdAt(LocalDateTime.now())
				.build();

			outboxRepository.save(outboxEntry);

		} catch (JsonProcessingException e) {
			throw new IllegalStateException("Erreur de sérialisation de l'événement Outbox", e);
		}
	}


	private CreateOrderResponse orderMapper(CustomerOrder order){
		return new CreateOrderResponse(
			order.getId().toString(),
			order.getStatus().name(),
			order.getLineItems().size(),
			order.getReceivedAt()
		);
	}

	private List<AllocationItem> findAllAllocationItem(List<UUID>lineItemUuids){
		   return  allocationItemService.findAllByLineItemIdInAndSkuIdNotNull(lineItemUuids);
	}


    /**
     * Déclenche l'allocation immédiatement (appel direct, sans Kafka).
     * Volontairement "best effort" : si l'allocation échoue, la commande reste néanmoins créée
     * (déjà commit via allocationService.allocate() en REQUIRES_NEW) — l'erreur est journalisée
     * et peut être rejouée plus tard, exactement comme l'ancien flux découplé par Kafka.
     */
    private void triggerAllocation(List<CustomerOrder> orders) {
         // OrderReceivedEvent event = orderReceivedEvent(order);
		// On publie l'événement au lieu d'appeler la méthode privée
		orders.stream()
			.forEach(order -> {
				// 1. Calcul du total pour chaque commande
				BigDecimal total = order.calculateTotal();
				log.debug("Commande ID: {} - Total calculé: {} €", order.getId(), total);

				// 2. Publication de l'événement
				// OrderReceivedEvent event = orderReceivedEvent(order);
				// kafkaEventPublisher.publishOrderINPROGRESS(event);
			});
		eventPublisher.publishEvent(orders);

    }


	private CustomerOrder order(CustomerOrder order){
		List<OrderReceivedEvent.OrderLine> lines = order.getLineItems().stream()
			.map(this::toOrderLine)
			.toList();

		BigDecimal total = order.calculateTotal();

		/**
		OrderReceivedEvent event = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(order.getId().toString())
			.customerId(order.getCustomerId())
			.lines(lines)
			.currency(order.getCurrency())
			.totalAmount(total)
			.priority(order.getPriority().name())
			.completeDeliveryRequired(order.isCompleteDeliveryRequired())
			.occurredAt(Instant.now())
			.build();
		 **/
		return order;

	}


	/**
	 * Libère le stock (partie critique, dans la transaction courante : si ça échoue, l'annulation
	 * doit échouer aussi) puis tente de rejouer les commandes en attente de ce stock (effet
	 * secondaire "best effort", isolé dans sa propre transaction par AllocationRetryService).
	 */
	private void releaseStockAndRetryPending(CustomerOrder order, List<AllocationItem> allocationItems) {

		// 2. Traitement du Retry dans une bulle isolée
		try {
			// muss call a rollback when something wrong append
			skuService.releaseBulkStock(allocationItems);

			// On appelle le service externe qui possède sa propre transaction isolée
			allocationRetryService.retryPendingOrders(allocationItems);
		} catch (Exception e) {
			// TRÈS IMPORTANT : On attrape l'exception ici.
			// Comme l'exception est gérée (catchée), elle ne remonte pas à la transaction principale.
			// Donc, skuService ne subira PAS de rollback même si le retry échoue.
			log.error("Le retry a échoué, mais la libération de stock est validée et conservée : {}", e.getMessage());
		}

	}



	private OrderReceivedEvent.OrderLine toOrderLine(LineItem li) {
		return OrderReceivedEvent.OrderLine.builder()
			// S'assurer que orderLineItemId représente la ligne
			.orderLineItemId(li.getId() != null ? li.getId().getValue().toString() : null)
			.orderId(li.getCustomerOrder() != null && li.getCustomerOrder().getId() != null
				? li.getCustomerOrder().getId().toString()
				: null)
			.sku(li.getProductNr() != null ? li.getProductNr().getValue() : null)
			.quantity(li.getRequestedQty() != null ? li.getRequestedQty().getValue() : 0)
			.unitPrice(li.getUnitPrice())
			.build();
	}

	private StockReleasedEvent.ReleasedLine toReleaseLine(AllocationItem allocationItem){
		return StockReleasedEvent.ReleasedLine.builder()
			.sku(allocationItem.getSkuId().toString())
			.lineItemId(allocationItem.getLineItemId().toString())
			.ProductNr(allocationItem.getProductNr().getValue())
			.quantityAllocated(allocationItem.getQuantity())
			.remainingQuantity(allocationItem.getRemainingQuantity())
			.build();

	}
}

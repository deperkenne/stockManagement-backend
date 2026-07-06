package com.stock.management.order;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.allocationLine.AllocationItemStatus;
import com.stock.management.kafka.event.OrderCancelledEvent;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockReleasedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.*;
import com.stock.management.order.internal.OrderValidator;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.stock.management.kafka.config.KafkaTopics.ALLOCATION_FAILED;
import static com.stock.management.order.domain.OrderStatus.CANCELLED;
import static com.stock.management.order.domain.OrderStatus.PARTIALLY_ALLOCATED;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;
	private final AllocationItemRepository allocationItemRepository;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final OrderValidator orderValidator;


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

    public CreateOrderResponse receiveOrder(CreateOrderRequest request) {
        orderValidator.validate(request);

        if (orderRepository.existsByExternalOrderNr(request.externalOrderNr())) {
            throw new DuplicateOrderException(
                    "Order already exists with externalOrderNr: " + request.externalOrderNr());
        }

        CustomerOrder order = CustomerOrder.create(
                request.externalOrderNr(),
                request.customerId(),
                request.priority(),
                request.completeDeliveryRequired(),
                request.currency(),
                request.lineItems()
        );

        order = orderRepository.save(order);

        log.info("[ORDER] Created orderId={} externalOrderNr={} lines={}",
                order.getId(), order.getExternalOrderNr(), order.getLineItems().size());

        publishOrderReceivedEvent(order);

        return new CreateOrderResponse(
                order.getId().toString(),
                order.getExternalOrderNr(),
                order.getStatus().name(),
                order.getLineItems().size(),
                order.getReceivedAt()
        );
    }

    // ─── Annulation commande complète ─────────────────────────────────────────────

	@Transactional
	public CancelOrderResponse cancelOrder(OrderId orderId,CancelOrderRequest request) {
		CustomerOrder order = orderRepository.findById(orderId)
			.orElseThrow(() -> new EntityNotFoundException("Order not found: " + orderId));

		OrderStatus oldStatus = order.getStatus();

		verifyStatus(oldStatus,order);

		/**
		 * Mutation de l'état et persistance uniquement si on n'est pas passé par le cas CANCELLED
		 */
		order.cancel(request.cancellationSource());
		orderRepository.save(order);

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

		// 1. Lock pessimiste sur le Parent (Order) pour éviter les conditions de concurrence (Race Conditions)
		CustomerOrder order = orderRepository.findByIdForUpdate(new OrderId(orderId))
			.orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

		// 2. Validations des règles de garde globales
		if (order.getStatus() == OrderStatus.CANCELLED) {
			throw new OrderCancellationException("Cannot modify a fully cancelled order: " + orderId);
		}
		if (order.getStatus() == OrderStatus.FULLY_ALLOCATED) {
			throw new OrderCancellationException("Cannot cancel items on a fully completed order: " + orderId);
		}

		// LE MÉTIER EST ENTIÈREMENT DÉLÉGUÉ À L'ENTITÉ ICI :
		List<LineItemId> targets = order.extractEligibleLineIdsForCancellation(request.lineItemIds());
		if (targets.isEmpty()) {
			log.info("[CANCEL] All requested lines are already cancelled for orderId={}", orderId);
			return new CancelOrderResponse(orderId.toString(), order.getStatus().name(), 0, request.cancellationSource().name(), order.getCancelledAt());
		}

		order.cancelLines(targets); //Change  orderLines status to Cancelled

		/**
		 * if  order has allocation_failed status  we look first how  we can change her status because one order could have a single line
		 * and if her status become cancelled we also change order status to cancelled and stop the programm
		 */
		if(order.getStatus() == OrderStatus.ALLOCATION_FAILED){
			order.evaluateAndModifyGlobalStatus();
			return new CancelOrderResponse(orderId.toString(), order.getStatus().name(), 0, request.cancellationSource().name(), order.getCancelledAt());
		}

		// Récupération et Soft-Delete des allocations associées pour éviter définitivement le rejeu
		List<AllocationItem> allocationsToRelease = allocationItemRepository.findAllByLineItemIdInAndSkuNotNull(targets);

		// 🗑️ Suppression des lignes d'allocation devenues obsolètes
		allocationItemRepository.deleteAllByLineItemIds(targets);

		// 6. Recalcul et ajustement du statut global de la commande
		order.evaluateAndModifyGlobalStatus();
		orderRepository.save(order);
		if (!allocationsToRelease.isEmpty()) {
			publishStockReleased(order, allocationsToRelease);
		}
		return new CancelOrderResponse(
			orderId.toString(),
			order.getStatus().name(),
			allocationsToRelease.size(),
			request.cancellationSource().name(),
			Instant.now()
		);
	}


	@Transactional
	public void handleCompleteDeliveryFailure(OrderId orderId) {
		// 1. On récupère UNIQUEMENT le statut actuel via une projection (SELECT très rapide sur index)
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

		log.info("[ORDER-BACKEND] Successfully transitioned order {} to ALLOCATION_FAILED. {} lines updated.", orderId, updatedLines);
	}


    // ─── Private helpers ──────────────────────────────────────────────────────────
    private CancelOrderResponse verifyStatus(OrderStatus oldStatus,CustomerOrder order){

		/**
		 * Utilisation du Switch Expression moderne (Java 14+)
		 * Avantage : Lisibilité linéaire maximale et zéro effet de bord.
		 */
		switch (oldStatus) {
			case CANCELLED -> {
				return new CancelOrderResponse(
					order.getCustomerId(),
					CANCELLED.name(),
					0,
					order.getCancellationSource() != null ? order.getCancellationSource().name() : null,
					order.getCancelledAt()
				);

			}
			case PARTIALLY_ALLOCATED -> {
				log.info("[CANCEL] Order {} is PARTIAL. Initiating stock release...", order.getCustomerId());
				releaseStockForPartialOrder(order);
			}
			case ALLOCATION_FAILED -> {
				log.info("[CANCEL] Order {} is FAILED. No stock was altered. Direct cancellation.", order.getCustomerId());
				throw new OrderCancellationException("Cannot cancel a completed order: " + order.getCustomerId());
			}
			default -> {
				// Optionnel mais recommandé pour les architectures résilientes :
				// Bloque les états imprévus (ex: READY_FOR_SHIPPING) qui ne devraient pas être annulés ainsi.
				log.warn("[CANCEL] Order {} is in status {}. Cancellation unhandled or rejected.", order.getCustomerId(), oldStatus);
				throw new IllegalStateException("Cannot cancel order in status: " + oldStatus);
			}
		}
		return null;
	}



	private void releaseStockForPartialLine(CustomerOrder order, List<LineItemId>lineItemIds){

	}

	private void releaseStockForPartialOrder(CustomerOrder order) {
		// Extraction des IDs de toutes les lignes de la commande
		List<LineItemId> lineItemIds = order.getLineItems().stream()
			.map(LineItem::getId)
			.toList();

		// 🔍 Récupération des allocations réelles existantes pour ces lignes
		List<AllocationItem> activeAllocations = allocationItemRepository.findAllByLineItemIdInAndSkuNotNull(lineItemIds);


		if (activeAllocations.isEmpty()) {
			log.info("[CANCEL] No active stock allocations found for partial order {}.", order.getId());
			return;
		}

		// 🗑️ Suppression des lignes d'allocation devenues obsolètes
		allocationItemRepository.deleteAllByLineItemIds(lineItemIds);

		publishStockReleased(order,activeAllocations);
	}

    /**
     * Charge l'agrégat Order avec ses lignes et allocations en une seule requête,
     * avec un lock pessimiste WRITE pour garantir l'atomicité des annulations concurrentes.
     */
    private CustomerOrder loadOrderForCancellation(UUID orderId) {
        return orderRepository.findByIdWithLineItemsAndAllocationsForUpdate(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
    }

    private CustomerOrder loadOrderWithLineItems(UUID orderId) {
        return orderRepository.findByIdWithLineItems(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
    }



    private void publishOrderCancelledEvent(CustomerOrder order,
                                             List<OrderCancelledEvent.CancelledAllocation> allocations,
                                             CancelOrderRequest request,
                                             OrderCancelledEvent.CancellationScope scope) {
        kafkaEventPublisher.publishOrderCancelled(OrderCancelledEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(order.getId().toString())
				.status(order.getStatus().toString())
                .customerId(order.getCustomerId())
                .reason(request.reason())
                .cancelledBy(request.cancelledBy())
                .cancellationSource(request.cancellationSource())
                .scope(scope)
                .allocations(allocations)
                .occurredAt(Instant.now())
				.completeDeliveryRequired(order.isCompleteDeliveryRequired())
                .build());
    }

    private void publishOrderReceivedEvent(CustomerOrder order) {
        List<OrderReceivedEvent.OrderLine> lines = order.getLineItems().stream()
                .map(this::toOrderLine)
                .toList();

        BigDecimal total = order.getLineItems().stream()
                .map(li -> li.getUnitPrice().multiply(BigDecimal.valueOf(li.getRequestedQty().getValue())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        kafkaEventPublisher.publishOrderReceived(OrderReceivedEvent.builder()
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

        log.debug("[ORDER] Published order.received for orderId={}", order.getId());
    }


	private void publishStockReleased(CustomerOrder order, List<AllocationItem>allocationItems){
		List<StockReleasedEvent.ReleasedLine> lines = allocationItems.stream()
			.map(this::toReleaseLine)
			.toList();
		kafkaEventPublisher.publishStockReleased(StockReleasedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(order.getId().toString())
			.releasedLines(lines)
			.occurredAt(Instant.now())
			.build());

		log.debug("[ORDER] Published order.received for orderId={}", order.getId());
	}

    private OrderReceivedEvent.OrderLine toOrderLine(LineItem li) {
        return OrderReceivedEvent.OrderLine.builder()
			    .orderLineItemId(li.getId().getValue().toString())
			    .orderId(li.getCustomerOrder().getCustomerId())
                .sku(li.getProductNr().getValue())
                .quantity(li.getRequestedQty().getValue())
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

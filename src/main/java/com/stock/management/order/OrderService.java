package com.stock.management.order;

import com.stock.management.kafka.event.OrderCancelledEvent;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.*;
import com.stock.management.order.internal.OrderValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;
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

    public CancelOrderResponse cancelOrder(UUID orderId, CancelOrderRequest request) {
        // Lock pessimiste : empêche deux annulations concurrentes sur la même commande
        CustomerOrder order = loadOrderForCancellation(orderId);

        // Idempotence : commande déjà annulée → répondre 200 sans rien refaire
        if (order.getStatus() == OrderStatus.CANCELLED) {
            log.info("[ORDER] cancelOrder — already cancelled, orderId={}", orderId);
            return new CancelOrderResponse(
                    orderId.toString(),
                    OrderStatus.CANCELLED.name(),
                    0,
                    order.getCancellationSource() != null ? order.getCancellationSource().name() : null,
                    order.getCancelledAt()
            );
        }

        if (order.getStatus() == OrderStatus.COMPLETED) {
            throw new OrderCancellationException("Cannot cancel a completed order: " + orderId);
        }

        // Collecter les allocations AVANT d'annuler — les lignes encore actives ont du stock alloué
        List<OrderCancelledEvent.CancelledAllocation> allocations =
                collectAllocations(order.getLineItems(), null);

        order.cancel(request.cancellationSource());

		// a revoire maxi 3 parameters
        publishOrderCancelledEvent(order, allocations, request, OrderCancelledEvent.CancellationScope.FULL_ORDER);

        log.info("[ORDER] Cancelled orderId={} source={} releasedAllocations={}",
                orderId, request.cancellationSource(), allocations.size());

        return new CancelOrderResponse(
                orderId.toString(),
                OrderStatus.CANCELLED.name(),
                allocations.size(),
                request.cancellationSource().name(),
                order.getCancelledAt()
        );
    }

    // ─── Annulation d'une seule ligne ─────────────────────────────────────────────

    public CancelOrderResponse cancelLineItem(UUID orderId, UUID lineItemId, CancelOrderRequest request) {
        // Lock pessimiste : empêche modification concurrente pendant l'annulation
        CustomerOrder order = loadOrderForCancellation(orderId);

        if (order.getStatus() == OrderStatus.CANCELLED) {
            throw new OrderCancellationException("Order is already fully cancelled: " + orderId);
        }
        if (order.getStatus() == OrderStatus.COMPLETED) {
            throw new OrderCancellationException("Cannot cancel a line item on a completed order: " + orderId);
        }

        LineItemId lid = new LineItemId(lineItemId);

        // Validation explicite : ligne introuvable → 404 ciblé
        LineItem lineItem = order.findLineItem(lid)
                .orElseThrow(() -> new LineItemNotFoundException(
                        "LineItem not found: " + lineItemId + " on order: " + orderId));

        // Idempotence : ligne déjà annulée → répondre 200 sans rien refaire
        if (!lineItem.isCancellable()) {
            log.info("[ORDER] cancelLineItem — already cancelled, orderId={} lineItemId={}", orderId, lineItemId);
            return new CancelOrderResponse(
                    orderId.toString(),
                    order.getStatus().name(),
                    0,
                    null,
                    order.getCancelledAt()
            );
        }

        // Collecter AVANT d'annuler — la ligne a encore son stock alloué
        List<OrderCancelledEvent.CancelledAllocation> allocations =
                collectAllocations(order.getLineItems(), lid);

        order.cancelLineItem(lid, request.cancellationSource());

        publishOrderCancelledEvent(order, allocations, request, OrderCancelledEvent.CancellationScope.SINGLE_LINE);

        log.info("[ORDER] LineItem cancelled orderId={} lineItemId={} source={} releasedAllocations={}",
                orderId, lineItemId, request.cancellationSource(), allocations.size());

        return new CancelOrderResponse(
                orderId.toString(),
                order.getStatus().name(),
                allocations.size(),
                request.cancellationSource().name(),
                order.getCancelledAt()
        );
    }

    // ─── Private helpers ──────────────────────────────────────────────────────────

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

    /**
     * Collecte les allocations des lignes actives à libérer.
     * Si lineItemId est non null → collecte uniquement pour cette ligne.
     * Les allocations sont déjà en mémoire grâce au JOIN FETCH dans loadOrderForCancellation.
     */
    private List<OrderCancelledEvent.CancelledAllocation> collectAllocations(
            List<LineItem> lines, LineItemId lineItemId) {

        return lines.stream()
                .filter(li -> li.getStatus() != LineItemStatus.CANCELLED)
                .filter(li -> lineItemId == null || li.getId().equals(lineItemId))
                .flatMap(li -> li.getAllocations().stream()
                        .map(a -> OrderCancelledEvent.CancelledAllocation.builder()
                                .skuId(a.getSkuId())
                                .locationId(a.getLocationId())
                                .productNr(li.getProductNr().getValue())
                                .quantity(a.getAllocatedQty().getValue())
                                .build()))
                .toList();
    }

    private void publishOrderCancelledEvent(CustomerOrder order,
                                             List<OrderCancelledEvent.CancelledAllocation> allocations,
                                             CancelOrderRequest request,
                                             OrderCancelledEvent.CancellationScope scope) {
        kafkaEventPublisher.publishOrderCancelled(OrderCancelledEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(order.getId().toString())
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

    private OrderReceivedEvent.OrderLine toOrderLine(LineItem li) {
        return OrderReceivedEvent.OrderLine.builder()
                .sku(li.getProductNr().getValue())
                .quantity(li.getRequestedQty().getValue())
                .unitPrice(li.getUnitPrice())
                .build();
    }
}

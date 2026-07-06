package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.event.StockReleasedEvent;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.domain.CustomerOrder;
import com.stock.management.order.domain.LineItem;
import com.stock.management.order.domain.OrderId;
import com.stock.management.order.domain.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
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
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class AllocationRetryService {

    private final OrderRepository orderRepository;
    private final AllocationItemRepository allocationItemRepository;
    private final KafkaEventPublisher kafkaEventPublisher;

    public void retryPendingOrders(StockReleasedEvent event) {
        if (event == null || event.getReleasedLines() == null) return;

        List<String> productNrs = event.getReleasedLines().stream()
            .map(StockReleasedEvent.ReleasedLine::getProductNr)
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
            .findByStatusAndLineProductNrs(OrderStatus.ALLOCATION_FAILED, productNrs); // refactory after with Kafka

        if (orders.isEmpty()) return;


        log.info("[RETRY] {} ALLOCATION_FAILED orders eligible for full replay", orders.size());
        orders.forEach(order -> safeRetry(order.getId().toString(), () -> republishFull(order)));
    }

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

        publish(order, lines);
        log.info("[RETRY] Full replay orderId={} lines={}", order.getId(), lines.size());
    }

    // ─── Case 2: WAITING_STOCK / NOT_ALLOCATED (skuId=null) ──────────────────────

    private void retryWaitingLines(List<String> productNrs) {
        List<AllocationItem> waiting = allocationItemRepository
            .findWaitingItemsByProductNrs(productNrs);

        if (waiting.isEmpty()) return;

        Map<UUID, List<AllocationItem>> byOrder = waiting.stream()
            .collect(Collectors.groupingBy(AllocationItem::getOrderId));

        log.info("[RETRY] {} PARTIALLY_ALLOCATED orders eligible for partial replay", byOrder.size());

        byOrder.forEach((orderId, items) ->
            safeRetry(orderId.toString(), () -> republishPartial(orderId, items)));
    }

    private void republishPartial(UUID rawOrderId, List<AllocationItem> waitingItems) {
        OrderId orderId = new OrderId(rawOrderId);
        CustomerOrder order = orderRepository.findByIdWithLineItems(orderId).orElse(null);
        if (order == null) return;

        // AllocationItem has no unit price — resolve from LineItem for totalAmount calculation
        Map<UUID, LineItem> lineMap = order.getLineItems().stream()
            .collect(Collectors.toMap(li -> li.getId().getValue(), li -> li));

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

        // Soft-delete stale records BEFORE publishing — AllocationItemService will create fresh ones
        allocationItemRepository.softDeleteWaitingByOrderId(rawOrderId);

        publish(order, lines);
        log.info("[RETRY] Partial replay orderId={} waitingLines={}", rawOrderId, lines.size());
    }

    // ─── Shared helpers ───────────────────────────────────────────────────────────

    private void publish(CustomerOrder order, List<OrderReceivedEvent.OrderLine> lines) {
        BigDecimal total = lines.stream()
            .map(l -> l.getUnitPrice().multiply(BigDecimal.valueOf(l.getQuantity())))
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
    }

    private void safeRetry(String orderId, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.error("[RETRY] Failed to re-queue orderId={}: {}", orderId, e.getMessage());
        }
    }
}

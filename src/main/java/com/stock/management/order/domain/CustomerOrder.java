package com.stock.management.order.domain;

import com.stock.management.order.dto.LineItemRequest;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ordercancelation consomme via une requette API POst duclient
 * order consome order orderAllocation topic pour changer le status
 *
 */


@Entity
@Table(name = "customer_orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CustomerOrder {

    @EmbeddedId
    private OrderId id;

    @Column(name = "external_order_nr", nullable = false, unique = true)
    private String externalOrderNr;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private OrderStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "priority", nullable = false, length = 20)
    private Priority priority;

    @Column(name = "complete_delivery_required", nullable = false)
    private boolean completeDeliveryRequired;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

   // @Column(name = "warehouse_id", nullable = false)
   // private String warehouseId;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    // Source de l'annulation — null tant que la commande n'est pas annulée
    @Enumerated(EnumType.STRING)
    @Column(name = "cancellation_source", length = 20)
    private CancellationSource cancellationSource;

    // Horodatage de l'annulation — null tant que la commande n'est pas annulée
    @Column(name = "cancelled_at")
    private Instant cancelledAt;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @OneToMany(mappedBy = "customerOrder", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<LineItem> lineItems = new ArrayList<>();

    // ─── Factory ─────────────────────────────────────────────────────────────────

    public static CustomerOrder create(
            String externalOrderNr,
            String customerId,
            //String warehouseId,
            Priority priority,
            boolean completeDeliveryRequired,
            String currency,
            List<LineItemRequest> lineItemRequests) {

        CustomerOrder order = new CustomerOrder();
        order.id = OrderId.generate();
        order.externalOrderNr = externalOrderNr;
        order.status = OrderStatus.PENDING;
        order.priority = priority;
        order.completeDeliveryRequired = completeDeliveryRequired;
        order.customerId = customerId;
        //order.warehouseId = warehouseId;
        order.currency = currency;
        order.receivedAt = Instant.now();

        for (LineItemRequest req : lineItemRequests) {
            LineItem li = LineItem.create(
                    order,
                    new ProductNr(req.productNr()),
                    new Quantity(req.requestedQty()),
                    req.unitPrice()
            );
            order.lineItems.add(li);
        }

        return order;
    }

    // ─── Business methods ─────────────────────────────────────────────────────────

    public boolean isCancellable() {
        return status != OrderStatus.CANCELLED && status != OrderStatus.FULLY_ALLOCATED;
    }

    /** Retrouve une ligne de commande par son ID — utile pour la validation côté service. */
    public Optional<LineItem> findLineItem(LineItemId lineItemId) {
        return lineItems.stream().filter(li -> li.getId().equals(lineItemId)).findFirst();
    }

    /** Annule toute la commande — toutes les lignes non encore annulées sont annulées. */
    public void cancel(CancellationSource source) {
        if (this.status == OrderStatus.FULLY_ALLOCATED) {
            throw new IllegalStateException("Cannot cancel a completed order: " + id);
        }
        this.status = OrderStatus.CANCELLED;
        this.cancelledAt = Instant.now();
        this.cancellationSource = source;
        lineItems.stream()
                .filter(li -> li.getStatus() != LineItemStatus.CANCELLED)
                .forEach(LineItem::cancel);
    }

    /**
     * Annule une seule ligne.
     * Si toutes les lignes sont désormais annulées, la commande entière passe à CANCELLED.
     */
    public void cancelLineItem(LineItemId lineItemId, CancellationSource source) {
        LineItem line = lineItems.stream()
                .filter(li -> li.getId().equals(lineItemId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("LineItem not found: " + lineItemId));

        if (line.getStatus() == LineItemStatus.CANCELLED) return;
        line.cancel();

        boolean allCancelled = lineItems.stream()
                .allMatch(li -> li.getStatus() == LineItemStatus.CANCELLED);
        if (allCancelled) {
            this.status = OrderStatus.CANCELLED;
            this.cancelledAt = Instant.now();
            this.cancellationSource = source;
        }
    }

    public void markInProgress() {
        this.status = OrderStatus.IN_PROGRESS;
    }

    public void updateAllocationStatus() {
        long fullyAllocated = lineItems.stream()
                .filter(li -> li.getStatus() == LineItemStatus.FULLY_ALLOCATED).count();
        long active = lineItems.stream()
                .filter(li -> li.getStatus() != LineItemStatus.CANCELLED).count();

        if (active == 0) return;
        if (fullyAllocated == active) this.status = OrderStatus.FULLY_ALLOCATED;
        else if (fullyAllocated > 0)  this.status = OrderStatus.PARTIALLY_ALLOCATED;
    }
}

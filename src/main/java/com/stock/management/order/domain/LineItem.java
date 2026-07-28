package com.stock.management.order.domain;

import com.stock.management.kafka.event.OrderReceivedEvent;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Entity
@Table(name = "line_items", indexes = {
        @Index(name = "idx_line_items_order_id", columnList = "order_id"),
        @Index(name = "idx_line_items_product_nr", columnList = "product_nr")
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LineItem {

    @EmbeddedId
    private LineItemId id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id", referencedColumnName = "id", nullable = false)
    private CustomerOrder customerOrder;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "product_nr", nullable = false))
    private ProductNr productNr;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "requested_qty", nullable = false))
    private Quantity requestedQty;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "allocated_qty", nullable = false))
    private Quantity allocatedQty;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private LineItemStatus status;

    // Verrou optimiste dédié à la ligne : sans lui, une commande chargée puis modifiée par deux
    // requêtes concurrentes (ex: annulation d'une ligne + résultat d'allocation Kafka) pourrait
    // silencieusement écraser l'une des deux mises à jour au flush.
    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

	@Column(name = "received_at", nullable = false, updatable = false)
	private Instant receivedAt;

	/**
	 * Automatisation Hibernate : juste avant le INSERT SQL,
	 * initialise allocatedAt et updatedAt en mémoire.
	 */
	@PrePersist
	private void onCreate() {
		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
		this.receivedAt = now;
		this.updatedAt = now;
	}

	/**
	 * Juste avant chaque UPDATE SQL, rafraîchit updatedAt.
	 */
	@PreUpdate
	private void onUpdate() {
		this.updatedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
	}

    static LineItem create(CustomerOrder order, ProductNr productNr, Quantity requestedQty, BigDecimal unitPrice) {
        LineItem li = new LineItem();
        li.id = LineItemId.generate();
        li.customerOrder = order;
        li.productNr = productNr;
        li.requestedQty = requestedQty;
        li.allocatedQty = Quantity.zero();
        li.unitPrice = unitPrice;
        li.status = LineItemStatus.PENDING;
        return li;
    }


    public void changeLineStatus(LineItemStatus status){
		this.status = status;
	}

    public boolean isCancellable() {
        return status != LineItemStatus.CANCELLED;
    }

    public void cancel() {
        if (this.status == LineItemStatus.CANCELLED) return;
        this.status = LineItemStatus.CANCELLED;
    }

    /** Modifie la quantité demandée et le prix unitaire (utilisé par CustomerOrder.updateLineItem). */
    public void updateDetails(Quantity requestedQty, BigDecimal unitPrice) {
        if (this.status == LineItemStatus.CANCELLED) {
            throw new IllegalStateException("Cannot update a cancelled line: " + id);
        }
        this.requestedQty = requestedQty;
        this.unitPrice = unitPrice;
    }
}

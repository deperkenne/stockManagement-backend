package com.stock.management.sku.domain;

import com.stock.management.order.domain.ProductNr;
import com.stock.management.order.domain.Quantity;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "skus")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Sku {

    @EmbeddedId
    private SkuId id;

    // ProductNr et Quantity sont des value objects partagés (order.domain).
    // À migrer vers un package shared/ si les BCs sont découplés en microservices.
    // Un même produit peut être stocké dans plusieurs emplacements → pas de unique
    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "product_nr", nullable = false))
    private ProductNr productNr;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "total_qty", nullable = false))
    private Quantity totalQuantity;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "available_qty", nullable = false))
    private Quantity availableQuantity;

    // Composition : WarehouseLocation ne peut pas exister sans son SKU
    @OneToOne(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @JoinColumn(name = "location_id", nullable = false)
    private WarehouseLocation location;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    // ─── Factory ─────────────────────────────────────────────────────────────────

    public static Sku create(ProductNr productNr, Quantity totalQuantity, String locationCode) {
        Sku sku = new Sku();
        sku.id = SkuId.generate();
        sku.productNr = productNr;
        sku.totalQuantity = totalQuantity;
        sku.availableQuantity = totalQuantity;
        sku.location = WarehouseLocation.create(locationCode);
        return sku;
    }

    // ─── Business methods ─────────────────────────────────────────────────────────

    public void reserve(Quantity qty) {
        if (location.isLocked()) {
            throw new IllegalStateException(
                    "Location " + location.getCode() + " is locked: " + location.getLockedReason());
        }
        if (qty.getValue() > availableQuantity.getValue()) {
            throw new IllegalStateException(
                    "Insufficient stock for " + productNr.getValue()
                    + ": available=" + availableQuantity.getValue()
                    + " requested=" + qty.getValue());
        }
        this.availableQuantity = new Quantity(availableQuantity.getValue() - qty.getValue());
    }

    public void release(Quantity qty) {
        int released = availableQuantity.getValue() + qty.getValue();
        if (released > totalQuantity.getValue()) {
            throw new IllegalStateException(
                    "Cannot release more than totalQuantity for " + productNr.getValue());
        }
        this.availableQuantity = new Quantity(released);
    }


    /** Réapprovisionnement physique : augmente total ET disponible. */
    public void replenish(Quantity qty) {
        this.totalQuantity     = new Quantity(totalQuantity.getValue() + qty.getValue());
        this.availableQuantity = new Quantity(availableQuantity.getValue() + qty.getValue());
    }

    public void lockLocation(String reason) {
        location.lock(reason);
    }

    public void unlockLocation() {
        location.unlock();
    }

    public boolean hasAvailableStock(Quantity requested) {
        return !location.isLocked() && availableQuantity.getValue() >= requested.getValue();
    }
}

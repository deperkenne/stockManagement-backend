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

    // Référence (pas composition) : l'emplacement vit indépendamment du SKU.
    // unique = true : au plus un SKU par emplacement (0..1 côté emplacement, 1 côté SKU).
    // Pas de REMOVE ni d'orphanRemoval : supprimer un SKU ne supprime jamais l'emplacement.
    // PERSIST/MERGE : permet encore de créer SKU + nouvel emplacement en un seul save (initialisation, tests).
    @ManyToOne( fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "location_id", nullable = false, unique = true)
    private WarehouseLocation location;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    // ─── Factory ─────────────────────────────────────────────────────────────────

    /** Crée un SKU dans un emplacement existant. */
    public static Sku create(ProductNr productNr, Quantity totalQuantity, WarehouseLocation location) {
        if (location == null) {
            throw new IllegalArgumentException("Location must not be null");
        }
        Sku sku = new Sku();
        sku.id = SkuId.generate();
        sku.productNr = productNr;
        sku.totalQuantity = totalQuantity;
        sku.availableQuantity = totalQuantity;
        sku.location = location;
        return sku;
    }

    /** Raccourci : crée le SKU et un nouvel emplacement en même temps (initialisation de données, tests). */
    public static Sku create(ProductNr productNr, Quantity totalQuantity, String locationCode) {
        return create(productNr, totalQuantity, WarehouseLocation.create(locationCode));
    }

    // ─── Business methods ─────────────────────────────────────────────────────────

    public void reserveQty(Quantity qty) {
        if (location.isLocked() ||  qty == null) {
            throw new IllegalStateException(
                    "Quantity  muss not be null or negative" );
        }
        if (qty.getValue() > availableQuantity.getValue()) {
            throw new IllegalStateException(
                    "Insufficient stock for " + productNr.getValue()
                    + ": available=" + availableQuantity.getValue()
                    + " requested=" + qty.getValue());
        }
        this.availableQuantity = new Quantity(availableQuantity.getValue() - qty.getValue());
    }

    public void releaseRemaningQty(Quantity qty) {
		if(qty.getValue() <= 0){
			throw new IllegalStateException(
				"Quantity  muss not be null or negative" );
		}
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

    public boolean hasAvailableStock(Quantity requested) {
        return !location.isLocked() && availableQuantity.getValue() >= requested.getValue();
    }

    /**
     * Déplace le stock vers un autre emplacement existant.
     * L'ancien emplacement n'est ni renommé ni supprimé : il redevient simplement libre.
     */
    public void moveTo(WarehouseLocation newLocation) {
        if (newLocation == null) {
            throw new IllegalArgumentException("Location must not be null");
        }
        this.location = newLocation;
    }

    /**
     * Mise à jour "CRUD" (PUT) : remplace productNr et totalQuantity (le déplacement passe par moveTo).
     * availableQuantity est ajustée du même delta que totalQuantity, pour ne jamais dépasser
     * ce qui est physiquement possible ni descendre sous 0 (protège le stock déjà réservé).
     */
    public void updateDetails(ProductNr productNr, Quantity totalQuantity) {
        int delta = totalQuantity.getValue() - this.totalQuantity.getValue();
        int newAvailable = this.availableQuantity.getValue() + delta;
        if (newAvailable < 0) {
            throw new IllegalStateException(
                    "Impossible de réduire totalQuantity en dessous de la quantité déjà réservée pour "
                    + productNr.getValue());
        }
        this.productNr = productNr;
        this.totalQuantity = totalQuantity;
        this.availableQuantity = new Quantity(newAvailable);
    }
}

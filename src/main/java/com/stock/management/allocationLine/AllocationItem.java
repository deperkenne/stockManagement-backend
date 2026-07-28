package com.stock.management.allocationLine;


import com.stock.management.order.domain.ProductNr;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "allocation_items", indexes = {
        @Index(name = "idx_allocation_items_order_id", columnList = "order_id"),
        @Index(name = "idx_allocation_items_line_item_id", columnList = "line_item_id"),
        @Index(name = "idx_allocation_items_product_nr", columnList = "product_nr")
})
@Data
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class AllocationItem {


		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		/** * Liaison logique : ID de la commande et référence du produit
		 */
		@Column(name = "order_id", nullable = false)
		private UUID orderId;

		@Column(name = "line_item_id", nullable = false)
		private UUID lineItemId;

		@Embedded
		@AttributeOverride(name = "value", column = @Column(name = "product_nr", nullable = false))
		private ProductNr productNr;

		@Column(name = "sku_id")
		private UUID skuId;

		@Column(name = "quantity", nullable = false)
		private int quantity;

	    @Column(name = "remaining_quantity", nullable = false)
	    private int remainingQuantity;

		@Enumerated(EnumType.STRING)
		@Column(name = "status", nullable = false)

		private AllocationItemStatus status;

	    @Column(name = "allocated_at", nullable = false, updatable = false)
		private Instant allocatedAt;

		// Verrou optimiste : cette ligne est écrite à la fois par le consommateur Kafka
		// (onStockAllocated) et par le CRUD de test — sans version, l'un des deux écrase l'autre en silence.
		@Version
		@Column(name = "version", nullable = false)
		private long version;

		@Column(name = "updated_at", nullable = false)
		private Instant updatedAt;

		// 🟢 L'ATTRIBUT DE SOFT DELETE
		@Column(name = "deleted", nullable = false)
		@Builder.Default
		private boolean deleted = false;

		// Horodatage du soft delete — null tant que "deleted" est false.
		@Column(name = "deleted_at")
		private Instant deletedAt;

		// Courte étiquette d'origine du soft delete : "LINE_CANCELLED", "SUPERSEDED_BY_RETRY"...
		// Traçabilité analytique : distingue une allocation libérée par annulation client
		// d'une allocation simplement remplacée par une nouvelle tentative de réapprovisionnement.
		@Column(name = "deletion_reason", length = 50)
		private String deletionReason;

	/**
	 * Automatisation Hibernate : juste avant le INSERT SQL,
	 * initialise allocatedAt et updatedAt en mémoire.
	 */
	@PrePersist
	private void onCreate() {
		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
		this.allocatedAt = now;
		this.updatedAt = now;
	}

	/**
	 * Juste avant chaque UPDATE SQL, rafraîchit updatedAt.
	 */
	@PreUpdate
	private void onUpdate() {
		this.updatedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
	}

	public void resetRemainingQty(Integer qty){
		this.remainingQuantity = qty;
	}

	public void changeStatus(AllocationItemStatus status){
		this.status = status;
	}

}

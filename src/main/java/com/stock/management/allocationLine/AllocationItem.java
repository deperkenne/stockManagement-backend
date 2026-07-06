package com.stock.management.allocationLine;


import com.stock.management.order.domain.ProductNr;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "allocation_items")
@Data
@Builder
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
		/**
		 * Date et heure exactes de la création de l'allocation venant de Kafka
		 */
		@Column(name = "allocated_at", nullable = false)
		private Instant allocatedAt;

		// 🟢 L'ATTRIBUT DE SOFT DELETE
		@Column(name = "deleted", nullable = false)
		@Builder.Default
		private boolean deleted = false;

}

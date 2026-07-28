package com.stock.management.order.domain;

import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.LineItemNotFoundException;
import com.stock.management.order.OrderCancellationException;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.LineItemRequest;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.stock.management.order.domain.LineItemStatus.NOT_ALLOCATED;

/**
 * ordercancelation consomme via une requette API POst duclient
 * order consome order orderAllocation topic pour changer le status
 *
 */


@Entity
@Table(name = "customer_orders", indexes = {
        @Index(name = "idx_customer_orders_status", columnList = "status")
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CustomerOrder {

    @EmbeddedId
    private OrderId id;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private OrderStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "priority", nullable = false, length = 20)
    private Priority priority;

    @Column(name = "complete_delivery_required", nullable = false)
    private boolean completeDeliveryRequired;

	@Column(name = "cancel-reason", nullable = true)
	private String cancelReason;

	@Column(name = "cancel-by", nullable = true)
	private String cancelBy;


    //@Column(name = "customer_id", nullable = false)
    //private String customerId;

   // @Column(name = "warehouse_id", nullable = false)
   // private String warehouseId;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;
    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;

    // Dernière modification — alimenté automatiquement par Hibernate (@PrePersist/@PreUpdate).
    // Sert de signal de fraîcheur pour l'analytique (latence de traitement = updatedAt - receivedAt).
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

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


	// =========================================================================
	// FACTORY METHOD OPTIMISÉE (Encapsule la création et protège le domaine)
	// =========================================================================
	public static CustomerOrder create(
		Priority priority,
		boolean completeDeliveryRequired,
		String currency,
		List<LineItemRequest> lineItemRequests) {

		CustomerOrder order = new CustomerOrder();
		order.id = OrderId.generate();
		order.status = OrderStatus.PENDING;
		order.priority = priority;
		order.completeDeliveryRequired = completeDeliveryRequired;
		order.currency = currency;

		Instant now = Instant.now();
		order.receivedAt = now;
		order.updatedAt = now;

		// Pré-dimensionnement de la liste pour éviter la réallocation mémoire
		order.lineItems = new ArrayList<>(lineItemRequests.size());

		for (LineItemRequest req : lineItemRequests) {
			// Helper method pour maintenir la cohérence bidirectionnelle
			order.addLineItem(req.productNr(), req.requestedQty(), req.unitPrice());
		}

		return order;
	}

	/**
	 * Garantit la cohérence bidirectionnelle JPA sans fuite d'encapsulation
	 */
	private void addLineItem(String productNr, int quantity, BigDecimal unitPrice) {
		LineItem li = LineItem.create(
			this,
			new ProductNr(productNr),
			new Quantity(quantity),
			unitPrice
		);
		this.lineItems.add(li);
	}
    // ─── Business methods ─────────────────────────────────────────────────────────

	/**
	 * Modifie le statut des lignes spécifiées vers CANCELLED.
	 * Cette méthode fait confiance à la liste filtrée en amont.
	 *
	 * @param targetsToCancel Liste des IDs des lignes à annuler.
	 */
	public void cancelLines(List<LineItemId> targetsToCancel) {
		if (targetsToCancel == null || targetsToCancel.isEmpty()) {
			return;
		}

		// Transformation en Set pour optimiser la recherche (.contains en O(1) au lieu de O(N))
		Set<LineItemId> targetSet = new HashSet<>(targetsToCancel);

		this.lineItems.stream()
			.filter(line -> targetSet.contains(line.getId()))
			.forEach(LineItem::cancel); // Délégation de la mutation à l'objet de transition (OrderLine)
	}



	/**
	 * Re-calcule de manière chirurgicale le statut global de la commande
	 * en fonction de l'état actuel de TOUTES ses lignes.
	 */
	public void evaluateAndModifyGlobalStatus() {
		// 1. Compte le nombre de lignes par statut
		long totalLines = this.lineItems.size();

		long cancelledLines = this.lineItems.stream()
			.filter(line -> line.getStatus() == LineItemStatus.CANCELLED)
			.count();

		long fullyAllocatedLines = this.lineItems.stream()
			.filter(line -> line.getStatus() == LineItemStatus.FULLY_ALLOCATED)
			.count();

		// 2. Machine à états (State Machine) comportementale
		if (cancelledLines == totalLines) {
			this.status = OrderStatus.CANCELLED; // Toutes les lignes sont annulées
		} else if (fullyAllocatedLines + cancelledLines == totalLines) {
			this.status = OrderStatus.FULLY_ALLOCATED; // Le reste est 100% alloué
		} else if (cancelledLines > 0) {
			this.status = OrderStatus.PARTIALLY_ALLOCATED; // Mutation naturelle suite à l'annulation partielle
		}
		// Tu peux rajouter tes autres règles ici sans impacter tes services
	}


	/**
	 * C'est l'entité qui prend la responsabilité complète du filtrage métier.
	 * Elle prend les IDs bruts et extrait uniquement ceux qui sont éligibles.
	 */
	public List<LineItemId> extractEligibleLineIdsForCancellation(List<UUID> requestedUuids) {


		return requestedUuids.stream()
			.map(LineItemId::new)
			.filter(this::isLineEligibleForCancellation) // Utilise la règle interne
			.toList();
	}

	private boolean isLineEligibleForCancellation(LineItemId lineItemId) {
		return this.lineItems.stream()
			.filter(line -> line.getId().equals(lineItemId))
			.findFirst()
			.map(line -> line.getStatus() != LineItemStatus.CANCELLED)
			.orElse(false);
	}

    public boolean isCancellable() {
        return status != OrderStatus.CANCELLED && status != OrderStatus.FULLY_ALLOCATED;
    }

    /** Retrouve une ligne de commande par son ID — utile pour la validation côté service. */
    public Optional<LineItem> findLineItem(LineItemId lineItemId) {
        return lineItems.stream().filter(li -> li.getId().equals(lineItemId)).findFirst();
    }

    /** Annule toute la commande — toutes les lignes non encore annulées sont annulées. */
    public void cancel(CancellationSource cancellationSource,String reason,String cancelBy) {
        if (this.status == OrderStatus.FULLY_ALLOCATED) {
            throw new IllegalStateException("Cannot cancel a completed order: " + id);
        }
        this.status = OrderStatus.CANCELLED;
		this.cancelReason = reason;
        this.cancelledAt = Instant.now();
        this.cancellationSource = cancellationSource;
		this.cancelBy = cancelBy;
        lineItems.stream()
                .filter(li -> li.getStatus() != LineItemStatus.CANCELLED)
                .forEach(LineItem::cancel);
    }

	public static OrderId createIdFrom(UUID rawUuid) {
		return new OrderId(rawUuid);
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

    // ─── CRUD (ajout / modification / suppression manuelle d'une ligne) ───────────

    /** Met à jour les champs modifiables de la commande. L'id, l'externalOrderNr et le status ne changent jamais ici. */
    public void updateDetails(Priority priority, boolean completeDeliveryRequired, String currency) {
        if (this.status == OrderStatus.CANCELLED) {
            throw new OrderCancellationException("Cannot update a cancelled order: " + id);
        }
        this.priority = priority;
        this.completeDeliveryRequired = completeDeliveryRequired;
        this.currency = currency;
    }

    /** Ajoute une nouvelle ligne à la commande existante. Interdit sur une commande déjà annulée. */
    public LineItem addLineItem(ProductNr productNr, Quantity requestedQty, BigDecimal unitPrice) {
        if (this.status == OrderStatus.CANCELLED) {
            throw new OrderCancellationException("Cannot add a line to a cancelled order: " + id);
        }
        LineItem li = LineItem.create(this, productNr, requestedQty, unitPrice);
        this.lineItems.add(li);
        return li;
    }

    /** Modifie la quantité demandée et le prix unitaire d'une ligne existante. */
    public void updateLineItem(LineItemId lineItemId, Quantity requestedQty, BigDecimal unitPrice) {
        LineItem line = findLineItem(lineItemId)
                .orElseThrow(() -> new LineItemNotFoundException("LineItem not found: " + lineItemId));
        line.updateDetails(requestedQty, unitPrice);
    }

    /** Retire définitivement une ligne de la commande — orphanRemoval=true déclenche le DELETE SQL au flush. */
    public void removeLineItem(LineItemId lineItemId) {
        LineItem line = findLineItem(lineItemId)
                .orElseThrow(() -> new LineItemNotFoundException("LineItem not found: " + lineItemId));
        this.lineItems.remove(line);
    }





   public void updateOrderStatus(OrderStatus orderStatus){
		    this.status = orderStatus;
   }

   private  Optional<LineItem> findLineById(UUID lineId){
	   return this.lineItems.stream()
		   .filter(item -> item.getId().getValue().equals(lineId))
		   .findFirst();
   }

   public void updateLineItemStatus( Map<UUID, LineItemStatus> lineStatusUpdates) {

	   for (Map.Entry<UUID, LineItemStatus> entry : lineStatusUpdates.entrySet()) {
		   UUID lineId = entry.getKey();
		   LineItemStatus newStatus = entry.getValue();

		   LineItem line = findLineById(lineId)
			   .orElseThrow(() -> new IllegalArgumentException(
				   "OrderLine " + lineId + " not found in CustomerOrder " + this.id
			   ));

		   // L'entité OrderLine gère sa propre validation / transition
		   line.changeLineStatus(newStatus);
	   }

	   // 🟢 FIN : Hibernate détecte les statuts modifiés en RAM et met à jour la BDD au commit
   }


	public  BigDecimal calculateTotal(){
		BigDecimal total = getLineItems().stream()

			.map(li -> li.getUnitPrice().multiply(BigDecimal.valueOf(li.getRequestedQty().getValue())))

			.reduce(BigDecimal.ZERO, BigDecimal::add);

		return total;
	}

}

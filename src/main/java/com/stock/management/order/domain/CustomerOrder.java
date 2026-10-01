package com.stock.management.order.domain;

import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.InvalidOrderStateException;
import com.stock.management.order.LineItemNotFoundException;
import com.stock.management.order.OrderCancellationException;
import com.stock.management.order.OrderStateConflictException;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.stock.management.order.domain.LineItemStatus.NOT_ALLOCATED;
import static com.stock.management.order.domain.OrderStatus.CANCELLED;

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

	/* optimisch lock pour proteger chaque commande d'une modification concurrente
    @Version
    @Column(name = "version", nullable = false)
    private long version;

	 */

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

    // ceci vas permettre au attaquant d'utiliser la fausse list ou la copie au lieu de la vrai
	public List<LineItem> getLineItems() {
		// Crée une vraie copie défensive ET immuable (Java 10+)
		return List.copyOf(this.lineItems);
	}

	// 2. Constructeur métier / package-private pour les tests et la création
	public  CustomerOrder(OrderId id, OrderStatus status, Priority priority, List<LineItem> lineItems) {
		this.id = Objects.requireNonNull(id, "Order ID cannot be null");
		this.status = (status != null) ? status : OrderStatus.PENDING;
		this.priority = (priority != null) ? priority : Priority.NORMAL;
		this.lineItems = lineItems != null ? new ArrayList<>(lineItems) : new ArrayList<>();
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
	 *
	 * @param targetsToCancel Liste des identifiants des lignes à annuler.
	 * @throws IllegalArgumentException si targetsToCancel est null.
	 */
	public void cancelLines(List<LineItemId> targetsToCancel) {
		if (targetsToCancel == null || targetsToCancel.isEmpty()) {
			throw new IllegalArgumentException("Target line items to cancel cannot be null or empty.");
		}

		// 2. Utilisation de Set.copyOf (Immuable, supprime les doublons, O(1) lookup)
		Set<LineItemId> targetSet = Set.copyOf(targetsToCancel);
		int cancelledCount = 0;
		// 2. Parcours impératif (pas de surcharge Stream)
		for (LineItem line : this.lineItems) {
			if (targetSet.contains(line.getId())) {
				line.cancel();
				cancelledCount++;
			}
		}

		// 4. Vérification d'intégrité : Tous les IDs demandés existaient-ils dans la commande ?
		if (cancelledCount != targetSet.size()) {
			throw new LineItemNotFoundException(
				String.format("Cancellation failed: Some line items do not belong to order %s", this.id.getValue())
			);
		}


	}



	/**
	 * Re-calcule de manière chirurgicale le statut global de la commande
	 * en fonction de l'état actuel de TOUTES ses lignes.
	 */

	public void evaluateAndModifyGlobalStatus() {
		if (this.lineItems == null || this.lineItems.isEmpty()) {
			throw new IllegalArgumentException("order muss habe least one lineitem");
		}

		int total = this.lineItems.size();
		int cancelled = 0;
		int fullyAllocated = 0;
		int partiallyAllocated = 0;

		// Un SEUL parcours de la liste O(N)
		for (LineItem line : this.lineItems) {
			if(line.getStatus()== null){
				throw  new IllegalArgumentException("item muss contain a status");
			}
			switch (line.getStatus()) {
				case CANCELLED -> cancelled++;
				case FULLY_ALLOCATED -> fullyAllocated++;
				case PARTIALLY_ALLOCATED -> partiallyAllocated++;
				case NOT_ALLOCATED -> { /* Compté implicitement dans total */ }
			}
		}

		// Machine à états explicite (Exhaustive)
		if (cancelled == total) {
			this.status = OrderStatus.CANCELLED;
		} else if (fullyAllocated + cancelled == total) {
			this.status = OrderStatus.FULLY_ALLOCATED;
		} else if (partiallyAllocated > 0 || fullyAllocated > 0 ) {
			this.status = OrderStatus.PARTIALLY_ALLOCATED;
		} else {
			this.status = OrderStatus.PENDING; // ou ALLOCATION_FAILED selon tes règles
		}
	}

	/**
	 * Extrait les LineItemId éligibles à l'annulation parmi les UUIDs demandés.
	 * Complexité : O(N + M) au lieu de O(N * M)
	 */
	public List<LineItemId> extractEligibleLineIdsForCancellation(List<UUID> requestedUuids) {
		if (requestedUuids == null || requestedUuids.isEmpty()) {
			return List.of();
		}

		else if(isEmptyOrNull(this.lineItems)){
			throw new NullPointerException("list item muss not be null");
		};

		// Lookup O(1) pour les UUIDs demandés
		Set<UUID> targetUuids = new HashSet<>(requestedUuids);

		return this.lineItems.stream()
			// 1. La ligne fait-elle partie de la demande ? (O(1))
			.filter(line -> targetUuids.contains(line.getId().getValue()))
			// 2. Est-elle dans un statut permettant l'annulation ?
			.filter(line -> line.getStatus() != LineItemStatus.CANCELLED)
			// 3. Extraction de l'ID métier
			.map(LineItem::getId)
			.toList();
	}
	/**
	 * C'est l'entité qui prend la responsabilité complète du filtrage métier.
	 * Elle prend les IDs bruts et extrait uniquement ceux qui sont éligibles.

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
	 */

    public boolean isCancellable() {
        return status != OrderStatus.CANCELLED && status != OrderStatus.FULLY_ALLOCATED;
    }

	/** Annule toute la commande — toutes les lignes non encore annulées sont annulées. */
	public void cancel(CancellationSource cancellationSource,String reason,String cancelBy) {
		if (this.status == OrderStatus.FULLY_ALLOCATED || this.status == CANCELLED ) {
			throw new IllegalStateException("Cannot cancel a completed or cancelled order: " + id);
		}
		else if(isEmptyOrNull(this.lineItems)){
			throw new NullPointerException("list item muss not be null");
		};
		this.status = OrderStatus.CANCELLED;
		this.cancelReason = reason;
		this.cancelledAt = Instant.now();
		this.cancellationSource = cancellationSource;
		this.cancelBy = cancelBy;
		lineItems.stream()
			.filter(li -> li.getStatus() != LineItemStatus.CANCELLED)
			.forEach(LineItem::cancel);
	}

	private boolean isEmptyOrNull(List<LineItem>lineItems){
			return (this.lineItems.isEmpty() || this.lineItems == null);
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

	    validateDetailsItem(lineItemId,requestedQty,unitPrice);

        LineItem line = findLineItem(lineItemId)
                .orElseThrow(() -> new LineItemNotFoundException("LineItem not found: " + lineItemId));

        line.updateDetails(requestedQty, unitPrice);
    }

	private void validateDetailsItem( LineItemId lineItemId, Quantity requestedQty, BigDecimal unitPrice){
		// 2. Pre-conditions / Defense against nulls
		Objects.requireNonNull(lineItemId, "lineItemId must not be null");
		Objects.requireNonNull(requestedQty, "requestedQty must not be null");
		Objects.requireNonNull(unitPrice, "unitPrice must not be null");

		//  (Optionnel) Validation des invariants de valeur
		if (unitPrice.compareTo(BigDecimal.ZERO) < 0) {
			throw new IllegalArgumentException("unitPrice cannot be negative: " + unitPrice);
		}

	}

    /** Retire définitivement une ligne de la commande — orphanRemoval=true déclenche le DELETE SQL au flush. */
    public void removeLineItem(LineItemId lineItemId) {
		Objects.requireNonNull(lineItemId, "lineItemId must not be null");
        LineItem line = findLineItem(lineItemId)
                .orElseThrow(() -> new LineItemNotFoundException("LineItem not found: " + lineItemId));
        this.lineItems.remove(line);
    }

	// probleme de security a gerer  apres
   public void updateOrderStatus(OrderStatus orderStatus){
		    if(this.status == OrderStatus.FULLY_ALLOCATED || this.status == CANCELLED){
				throw new InvalidOrderStateException("Impossible d'annuler une commande déjà expédiée ou deja supprimer");
			}
		    this.status = orderStatus;
   }



   public void updateLineItemStatus( Map<UUID, LineItemStatus> lineStatusUpdates) {

	   // 1. Validations & Pre-conditions
	   validateLineStatusToUpdate(lineStatusUpdates);


	   // 3. OPTIMISATION ALGORITHMIQUE : Indexation en O(M)
	   // Transforme la List<LineItem> en Map<UUID, LineItem> pour un accès O(1)
	   Map<UUID, LineItem> lineItemMap = indexLineItemsById();

	   for (Map.Entry<UUID, LineItemStatus> entry : lineStatusUpdates.entrySet()) {
		   UUID lineId = entry.getKey();
		   LineItemStatus newStatus = entry.getValue();

		   // Recherche O(1) au lieu de O(M)
		   LineItem line = lineItemMap.get(lineId);
		   if (line == null) {
			   throw new IllegalArgumentException(
				   "LineItem [" + lineId + "] not found in CustomerOrder [" + this.id.getValue() + "]"
			   );
		   }

		   try {
			   // L'entité OrderLine gère sa propre validation / transition
			   line.changeLineStatus(newStatus);
		   } catch (Exception ex) {
			   // Loggez le contexte exact pour corriger la donnée
			   throw new IllegalStateException(
				   String.format("Échec du changement de statut pour la ligne %s vers %s dans la commande %s. Cause: %s",
					   lineId, newStatus, this.id.getValue(), ex.getMessage()), ex
			   );
		   }
	   }

   }

	public void validateOrderStateForCancellation() {
		if (this.status == OrderStatus.CANCELLED || this.status == OrderStatus.FULLY_ALLOCATED) {
			throw new OrderCancellationException("Cannot modify a fully cancelled order with status: " + this.status);
		}
	}

	public void validateCancellationEligibility() {
		if (status == OrderStatus.FULLY_ALLOCATED || status == CANCELLED) {
			throw new OrderCancellationException("Cannot cancel a completed or cancelled order: " + status);
		}
	}

	/**
	 * Extrait directement les valeurs d'identifiants des lignes de commande.
	 */
	public List<UUID> extractLineItemUuids() {
		return this.lineItems.stream()
			.map(line -> line.getId().getValue())
			.toList();
	}

	/**
	 * 🔒 Détail d'implémentation (Helper privé)
	 * Indexe la liste des lignes de commande en Map O(1) pour accélérer les recherches.
	 */
	private Map<UUID, LineItem> indexLineItemsById() {
		return this.lineItems.stream()
			.collect(Collectors.toMap(
				item -> item.getId().getValue(),
				Function.identity()
			));
	}

   private void  validateLineStatusToUpdate(Map<UUID, LineItemStatus> lineStatusUpdates){
	   Objects.requireNonNull(lineStatusUpdates, "lineStatusUpdates map must not be null");
	   if (lineStatusUpdates.isEmpty()) {
		   return;
	   }
   }

   private void validateOrderStatus(OrderStatus orderStatus){
	   if (this.status == OrderStatus.CANCELLED || this.status == OrderStatus.FULLY_ALLOCATED) {
		   throw new OrderStateConflictException(
			   "Cannot update line items for order [" + this.id.getValue() + "] in status " + this.status
		   );
	   }
   }

	// Dans CustomerOrder.java
	private Optional<LineItem> findLineItem(LineItemId lineItemId) {
		if (lineItemId == null) return Optional.empty();

		for (LineItem item : this.lineItems) {
			if (item.getId().equals(lineItemId)) {
				return Optional.of(item);
			}
		}
		return Optional.empty();
	}

	public  BigDecimal calculateTotal(){
		BigDecimal total = getLineItems().stream()

			.map(li -> li.getUnitPrice().multiply(BigDecimal.valueOf(li.getRequestedQty().getValue())))

			.reduce(BigDecimal.ZERO, BigDecimal::add);

		return total;
	}

}

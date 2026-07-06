package com.stock.management.order;

import com.stock.management.order.domain.*;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;


// exercice corriger toute les requette inutile qui entraine le lock et aller et retour inutile
// et qui ralenti la performance

public interface OrderRepository extends JpaRepository<CustomerOrder, OrderId> {

    boolean existsByExternalOrderNr(String externalOrderNr);
	/**
	 * Récupère uniquement le statut d'une commande par son ID.
	 * Évite le chargement complet de l'entité et de ses relations (Performance).
	 */
	@Query("SELECT o.status FROM CustomerOrder o WHERE o.id = :id")
	Optional<OrderStatus> findStatusById(@Param("id") OrderId id);

    Optional<CustomerOrder> findByExternalOrderNr(String externalOrderNr);

	@Modifying
	@Query("UPDATE Order o SET o.status = :newStatus WHERE o.id = :orderId AND o.status != :newStatus")
	int updateOrderStatusIfChanged(@Param("orderId") OrderId orderId, @Param("newStatus") OrderStatus newStatus);

	@Modifying
	@Query("UPDATE LineItem l SET l.status = :lineStatus WHERE l.order.id = :orderId AND l.status != :lineStatus")
	int updateAllLinesStatusIfChanged(@Param("orderId") OrderId orderId, @Param("lineStatus") LineItemStatus lineStatus);


	/**
	 * Met à jour le statut de toutes les lignes d'une commande spécifique en une seule requête SQL.
	 * clearAutomatically = true évite les effets de bord avec le cache de premier niveau (Persistence Context).
	 */
	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("UPDATE LineItem l SET l.status = :status WHERE l.orderId = :orderId")
	int updateAllLinesStatus(@Param("orderId") OrderId orderId, @Param("status") LineItemStatus status);



	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("UPDATE Order o SET o.status = :status WHERE o.id = :orderId")
	int updateOrderStatus(@Param("orderId") OrderId orderId, @Param("status") OrderStatus status);

	@Modifying
	@Query("UPDATE OrderLine l SET l.status = :lineStatus WHERE l.orderId = :orderId AND l.id = :lineId")
	int updateLineStatus(
		@Param("orderId") OrderId orderId,
		@Param("lineId") LineItemId lineId,
		@Param("lineStatus") LineItemStatus lineItemStatus
		);

    @Modifying
    @Query("UPDATE Order o SET o.status = :status WHERE o.id.value IN :ids")
    int updateStatusForIds(@Param("ids") List<OrderId> ids, @Param("status") OrderStatus status);

    @Query("SELECT o FROM CustomerOrder o LEFT JOIN FETCH o.lineItems WHERE o.id = :id")
    Optional<CustomerOrder> findByIdWithLineItems(@Param("id") OrderId id);

    /**
     * Finds orders in a given status whose lines include at least one of the given productNrs.
     * Used by AllocationRetryService to replay ALLOCATION_FAILED orders after stock is released.
     * EXISTS subquery ensures all lineItems are fetched, not just the matching ones.
     */
    @Query("""
            SELECT DISTINCT o FROM CustomerOrder o
            JOIN FETCH o.lineItems
            WHERE o.status = :status
            AND EXISTS (
                SELECT l FROM LineItem l
                WHERE l.customerOrder = o
                AND l.productNr.value IN :productNrs
            )
            """)
    List<CustomerOrder> findByStatusAndLineProductNrs(
            @Param("status") OrderStatus status,
            @Param("productNrs") List<String> productNrs
    );


    /**
     * Charge la commande avec ses lignes ET leurs allocations en une seule requête.
     * Lock pessimiste WRITE pour garantir l'atomicité lors d'une annulation concurrente.
     * DISTINCT évite la duplication de l'agrégat due aux JOINs imbriqués.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT DISTINCT o FROM CustomerOrder o
            LEFT JOIN FETCH o.lineItems li
            LEFT JOIN FETCH li.allocations
            WHERE o.id = :id
            """)
    Optional<CustomerOrder> findByIdWithLineItemsAndAllocationsForUpdate(@Param("id") OrderId id);


	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("SELECT o FROM CustomerOrder o WHERE o.id = :id")
	Optional<CustomerOrder> findByIdForUpdate(@Param("id") OrderId id);

	@Modifying(clearAutomatically = true)
	@Query("UPDATE LineItem l SET l.status = :status WHERE l.orderId = :orderId AND l.id IN :lineIds")
	void updateLinesStatus(UUID orderId, List<LineItemId> lineIds, LineItemStatus status);
}


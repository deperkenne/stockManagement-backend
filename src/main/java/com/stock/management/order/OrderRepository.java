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

public interface OrderRepository extends JpaRepository<CustomerOrder, OrderId> {

    boolean existsByExternalOrderNr(String externalOrderNr);

    Optional<CustomerOrder> findByExternalOrderNr(String externalOrderNr);


	/**
	 * Met à jour le statut de toutes les lignes d'une commande spécifique en une seule requête SQL.
	 * clearAutomatically = true évite les effets de bord avec le cache de premier niveau (Persistence Context).
	 */
	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("UPDATE LineItem l SET l.status = :status WHERE l.orderId = :orderId")
	int updateAllLinesStatus(@Param("orderId") UUID orderId, @Param("status") LineItemStatus status);



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
}


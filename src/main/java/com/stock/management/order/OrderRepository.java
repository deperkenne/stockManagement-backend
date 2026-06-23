package com.stock.management.order;

import com.stock.management.order.domain.CustomerOrder;
import com.stock.management.order.domain.OrderId;
import com.stock.management.order.domain.OrderStatus;
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
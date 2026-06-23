package com.stock.management.allocation;

import com.stock.management.order.domain.OrderId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AllocationFailedOrderRepo extends JpaRepository<AllocationFailedOrder, OrderId> {
		// Trouver toutes les commandes EN ATTENTE qui contiennent un SKU spécifique
		// Utile lorsque le stock de ce SKU est modifié (réapprovisionnement)
		@Query("SELECT DISTINCT fo FROM AllocationFailedOrder fo " +
			"JOIN fo.lines l " +
			"WHERE fo.status = :status AND l.sku IN :skus")
		List<AllocationFailedOrder> findPendingOrdersBySkus(
			@Param("skus") List<String> skus,
			@Param("status") AllocationFailedOrder.ProcessedStatus status
		) ;
	}


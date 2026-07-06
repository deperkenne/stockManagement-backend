package com.stock.management.allocationLine;

import com.stock.management.order.domain.LineItemId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface AllocationItemRepository extends JpaRepository<AllocationItem, Long> {

	List<AllocationItem> findAllByLineItemIdInAndSkuNotNull(List<LineItemId> lineItemIds);

	@Modifying
	@Query("UPDATE StockAllocation sa SET sa.deleted = true WHERE sa.lineItemId IN :lineItemIds AND sa.deleted = false")
	void deleteAllByLineItemIds(@Param("lineItemIds") List<LineItemId> lineItemIds);

	/**
	 * Lines still waiting for stock: no SKU was ever matched (skuId IS NULL = NOT_ALLOCATED)
	 * or stock was partially found but remainingQuantity > 0 (WAITING_STOCK).
	 * deleted = false excludes cancelled lines.
	 */
	@Query("""
			SELECT a FROM AllocationItem a
			WHERE a.productNr.value IN :productNrs
			AND (a.skuId IS NULL OR a.remainingQuantity > 0)
			AND a.deleted = false
			""")
	List<AllocationItem> findWaitingItemsByProductNrs(@Param("productNrs") List<String> productNrs);

	/** Marks stale WAITING/NOT_ALLOCATED records as deleted before fresh allocation creates new ones. */
	@Modifying(clearAutomatically = true)
	@Query("""
			UPDATE AllocationItem a SET a.deleted = true
			WHERE a.orderId = :orderId
			AND (a.skuId IS NULL OR a.remainingQuantity > 0)
			AND a.deleted = false
			""")
	void softDeleteWaitingByOrderId(@Param("orderId") UUID orderId);
}


package com.stock.management.allocationLine;

import com.stock.management.order.domain.AllocationStatus;
import com.stock.management.order.domain.ProductNr;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface AllocationItemRepository extends JpaRepository<AllocationItem, Long> {

	/**
	 * Le nom de propriété doit être "SkuId" (champ réel de l'entité) et non "Sku" : sans ce
	 * correctif, Spring Data échoue à démarrer (PropertyReferenceException, propriété inexistante).
	 * Le paramètre est un UUID brut, pas un LineItemId : la colonne lineItemId n'est pas un
	 * @EmbeddedId, un LineItemId ne peut donc pas s'y binder directement.
	 */
	List<AllocationItem> findAllByLineItemIdInAndSkuIdNotNull(List<UUID> lineItemIds);


	/**
	 * Annule toutes les lignes d'allocation associées à une commande spécifique.
	 * Passe leur statut à 'CANCELLED' en une seule requête SQL.
	 */
	@Modifying(clearAutomatically = true)
	@Query("""
      UPDATE AllocationItem a
      SET a.status = :cancelled, a.updatedAt = :now
      WHERE a.orderId = :orderId AND a.status != :cancelled
      """)
	int cancelAllItemsByOrderId(@Param("orderId") UUID orderId,
								@Param("cancelled") AllocationItemStatus cancelled,
								@Param("now") Instant now);
	/**
	 * Annule les allocations d'une ligne annulée.
	 * Met à jour le statut des items d'allocation à 'CANCELLED' de manière groupée.
	 */
	@Modifying(clearAutomatically = true)
	@Query("""
      UPDATE AllocationItem a
      SET a.status = :cancelled, a.updatedAt = :now
      WHERE a.lineItemId IN :lineItemIds AND a.status != :cancelled
      """)
	int cancelAllByLineItemIds(@Param("lineItemIds") List<UUID> lineItemIds,
							   @Param("cancelled") AllocationItemStatus cancelled,
							   @Param("now") Instant now);
	/**
	 * Lines still waiting for stock: no SKU was ever matched (skuId IS NULL = NOT_ALLOCATED)
	 * or stock was partially found but remainingQuantity > 0 (WAITING_STOCK).
	 * deleted = false excludes cancelled lines.
	 */
	@Query("""
       SELECT a FROM AllocationItem a
       WHERE a.productNr IN :productNrs
       AND (a.skuId IS NULL OR a.remainingQuantity > 0)
       AND a.status = :status
       """)
	List<AllocationItem> findWaitingItemsByProductNrs(
		@Param("productNrs") List<String> productNrs,
		@Param("status") AllocationItemStatus waitingStock);

	/** Marks stale WAITING/NOT_ALLOCATED records as deleted before fresh allocation creates new ones. */
	@Modifying(clearAutomatically = true)
	@Query("""
      UPDATE AllocationItem a
      SET a.deleted = true, a.deletedAt = :now, a.deletionReason = 'SUPERSEDED_BY_RETRY'
      WHERE a.orderId = :orderId
      AND (a.skuId IS NULL OR a.remainingQuantity > 0)
      AND a.deleted = false
      """)
	int softDeleteWaitingByOrderId(@Param("orderId") UUID orderId, @Param("now") Instant now);
}

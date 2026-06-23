package com.stock.management.sku;

import com.stock.management.order.domain.ProductNr;
import com.stock.management.sku.domain.Sku;
import com.stock.management.sku.domain.SkuId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;

public interface SkuRepository extends JpaRepository<Sku, SkuId> {

    // Un produit peut avoir plusieurs SKUs (emplacements différents)
    List<Sku> findByProductNr(ProductNr productNr);

    // Vérifie si un SKU existe déjà pour ce produit dans cet emplacement précis
    @Query("SELECT COUNT(s) > 0 FROM Sku s WHERE s.productNr.value = :productNr AND s.location.code = :locationCode")
    boolean existsByProductNrAndLocationCode(@Param("productNr") String productNr,
                                              @Param("locationCode") String locationCode);

    // Pessimistic lock pour les opérations de réservation/libération concurrentes
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM Sku s WHERE s.id = :id")
    Optional<Sku> findByIdForUpdate(@Param("id") SkuId id);

    /**
     * Retourne tous les SKUs disponibles pour un produit donné :
     * - emplacement non verrouillé
     * - quantité disponible > 0
     * Triés par quantité disponible DESC (greedy : vider les plus pleins en premier).
     * PESSIMISTIC_WRITE pour éviter les double-allocations concurrentes.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT s FROM Sku s
            JOIN s.location l
            WHERE s.productNr.value = :productNr
            AND l.locked = false
            AND s.availableQuantity.value > 0
            ORDER BY s.availableQuantity.value DESC
            """)
    List<Sku> findAvailableSkusForAllocation(@Param("productNr") String productNr);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT s FROM Sku s
            JOIN FETCH s.location l
            WHERE s.productNr.value IN :skuCodes
            AND l.locked = false
            AND s.availableQuantity.value > 0
            ORDER BY s.productNr.value ASC, s.availableQuantity.value DESC
            """)
    List<Sku> findAvailableSkusForAllocationWithLock(@Param("skuCodes") List<String> skuCodes);
}

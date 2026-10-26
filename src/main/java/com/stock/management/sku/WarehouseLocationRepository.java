package com.stock.management.sku;

import com.stock.management.sku.domain.LocationId;
import com.stock.management.sku.domain.WarehouseLocation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface WarehouseLocationRepository extends JpaRepository<WarehouseLocation, LocationId> {

    /** Le code doit déjà être normalisé (WarehouseLocation.normalizeCode). */
    Optional<WarehouseLocation> findByCode(String code);

    boolean existsByCode(String code);
}

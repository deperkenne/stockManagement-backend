package com.stock.management.sku;

import com.stock.management.sku.domain.LocationId;
import com.stock.management.sku.domain.WarehouseLocation;
import com.stock.management.sku.dto.CreateLocationRequest;
import com.stock.management.sku.dto.LocationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Gestion des emplacements physiques de l'entrepôt, indépendamment du stock qu'ils contiennent. */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class WarehouseLocationService {

    private final WarehouseLocationRepository locationRepository;
    private final SkuRepository skuRepository;

    @Transactional
    public LocationResponse create(CreateLocationRequest request) {
        String code = WarehouseLocation.normalizeCode(request.code());
        if (locationRepository.existsByCode(code)) {
            throw new LocationConflictException("Location already exists: " + code);
        }
        WarehouseLocation location = locationRepository.save(WarehouseLocation.create(code));
        log.info("[LOCATION] Created locationId={} code={}", location.getId(), code);
        return toResponse(location, false);
    }

    public List<LocationResponse> findAll() {
        Set<LocationId> occupied = new HashSet<>(skuRepository.findAllOccupiedLocationIds());
        return locationRepository.findAll().stream()
                .map(l -> toResponse(l, occupied.contains(l.getId())))
                .toList();
    }

    public LocationResponse findById(UUID id) {
        WarehouseLocation location = getLocation(id);
        return toResponse(location, skuRepository.existsByLocationId(location.getId()));
    }

    /** Change l'étiquette de l'emplacement. Le stock qu'il contient reste au même endroit. */
    @Transactional
    public LocationResponse rename(UUID id, CreateLocationRequest request) {
        WarehouseLocation location = getLocation(id);
        String newCode = WarehouseLocation.normalizeCode(request.code());
        if (!newCode.equals(location.getCode()) && locationRepository.existsByCode(newCode)) {
            throw new LocationConflictException("Location already exists: " + newCode);
        }
        location.rename(newCode);
        log.info("[LOCATION] Renamed locationId={} to code={}", id, newCode);
        return toResponse(location, skuRepository.existsByLocationId(location.getId()));
    }

    /** Bloque l'emplacement : son stock n'est plus proposé à l'allocation. */
    @Transactional
    public LocationResponse lock(UUID id, String reason) {
        WarehouseLocation location = getLocation(id);
        location.lock(reason);
        log.info("[LOCATION] Locked locationId={} reason={}", id, reason);
        return toResponse(location, skuRepository.existsByLocationId(location.getId()));
    }

    @Transactional
    public LocationResponse unlock(UUID id) {
        WarehouseLocation location = getLocation(id);
        location.unlock();
        log.info("[LOCATION] Unlocked locationId={}", id);
        return toResponse(location, skuRepository.existsByLocationId(location.getId()));
    }

    /** Suppression autorisée uniquement si aucun SKU n'occupe l'emplacement. */
    @Transactional
    public void
	delete(UUID id) {
        WarehouseLocation location = getLocation(id);
        if (skuRepository.existsByLocationId(location.getId())) {
            throw new LocationConflictException(
                    "Location " + location.getCode() + " still contains stock: move or delete its SKU first");
        }
        locationRepository.delete(location);
        log.info("[LOCATION] Deleted locationId={}", id);
    }

    // ─── Private helpers ──────────────────────────────────────────────────────────

    private WarehouseLocation getLocation(UUID id) {
        return locationRepository.findById(new LocationId(id))
                .orElseThrow(() -> new LocationNotFoundException("Location not found: " + id));
    }

    private LocationResponse toResponse(WarehouseLocation location, boolean occupied) {
        return new LocationResponse(
                location.getId().toString(),
                location.getCode(),
                location.isLocked(),
                location.getLockedReason(),
                occupied
        );
    }
}

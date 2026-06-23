package com.stock.management.sku.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "warehouse_locations")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class WarehouseLocation {

    @EmbeddedId
    private LocationId id;

    @Column(name = "code", nullable = false, unique = true)
    private String code;

    @Column(name = "locked", nullable = false)
    private boolean locked;

    @Column(name = "locked_reason")
    private String lockedReason;

    // ─── Factory (package-private — only Sku can create a location) ──────────────

    static WarehouseLocation create(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("Location code must not be blank");
        }
        WarehouseLocation wl = new WarehouseLocation();
        wl.id = LocationId.generate();
        wl.code = code.trim().toUpperCase();
        wl.locked = false;
        return wl;
    }

    // ─── Business methods (package-private — only accessible through Sku) ────────

    void lock(String reason) {
        this.locked = true;
        this.lockedReason = reason;
    }

    void unlock() {
        this.locked = false;
        this.lockedReason = null;
    }
}
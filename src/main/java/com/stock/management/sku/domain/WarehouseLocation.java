package com.stock.management.sku.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Emplacement physique de l'entrepôt (ex : "A-01").
 * Agrégat indépendant : il existe avant, pendant et après le stock qu'il contient.
 * Un Sku le référence, mais ne le possède pas (supprimer un Sku ne supprime pas l'emplacement).
 */
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

    // ─── Factory ─────────────────────────────────────────────────────────────────

    public static WarehouseLocation create(String code) {
        WarehouseLocation wl = new WarehouseLocation();
        wl.id = LocationId.generate();
        wl.code = normalizeCode(code);
        wl.locked = false;
        return wl;
    }

    /** Format unique des codes (trim + majuscules) : à utiliser aussi pour toute recherche par code. */
    public static String normalizeCode(String code) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("Location code must not be blank");
        }
        return code.trim().toUpperCase();
    }

    // ─── Business methods ─────────────────────────────────────────────────────────

    /** Bloque l'emplacement (inventaire, casse...) : son stock n'est plus allouable. */
    public void lock(String reason) {
        this.locked = true;
        this.lockedReason = reason;
    }

    public void unlock() {
        this.locked = false;
        this.lockedReason = null;
    }

    /** Change l'étiquette de l'emplacement lui-même (ne déplace aucun stock). */
    public void rename(String code) {
        this.code = normalizeCode(code);
    }
}

package com.stock.management.sku.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class LocationId implements Serializable {

    @Column(name = "id", updatable = false, nullable = false)
    private UUID value;

    protected LocationId() {}

    public LocationId(UUID value) {
        this.value = Objects.requireNonNull(value, "LocationId must not be null");
    }

    public static LocationId generate() {
        return new LocationId(UUID.randomUUID());
    }

    public UUID getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LocationId other)) return false;
        return Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : "null";
    }
}
package com.stock.management.sku.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class SkuId implements Serializable {

    @Column(name = "id", updatable = false, nullable = false)
    private UUID value;

    protected SkuId() {}

    public SkuId(UUID value) {
        this.value = Objects.requireNonNull(value, "SkuId must not be null");
    }

    public static SkuId generate() {
        return new SkuId(UUID.randomUUID());
    }

    public UUID getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SkuId other)) return false;
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
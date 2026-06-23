package com.stock.management.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class AllocationId implements Serializable {

    @Column(name = "id", updatable = false, nullable = false)
    private UUID value;

    protected AllocationId() {}

    public AllocationId(UUID value) {
        this.value = Objects.requireNonNull(value, "AllocationId must not be null");
    }

    public static AllocationId generate() {
        return new AllocationId(UUID.randomUUID());
    }

    public UUID getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AllocationId other)) return false;
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
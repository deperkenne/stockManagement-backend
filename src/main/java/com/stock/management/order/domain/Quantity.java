package com.stock.management.order.domain;

import jakarta.persistence.Embeddable;

import java.util.Objects;

@Embeddable
public class Quantity {

    private int value;

    protected Quantity() {}

    public Quantity(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Quantity must not be negative, got: " + value);
        }
        this.value = value;
    }

    public static Quantity zero() {
        return new Quantity(0);
    }

    public int getValue() {
        return value;
    }

    public Quantity add(Quantity other) {
        return new Quantity(this.value + other.value);
    }

    public boolean isFullyAllocated(Quantity requested) {
        return this.value >= requested.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Quantity other)) return false;
        return value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
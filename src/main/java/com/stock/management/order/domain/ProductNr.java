package com.stock.management.order.domain;

import jakarta.persistence.Embeddable;

import java.util.Objects;

@Embeddable
public class ProductNr {

    private String value;

    protected ProductNr() {}

    public ProductNr(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("ProductNr must not be blank");
        }
        this.value = value.trim().toUpperCase();
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductNr other)) return false;
        return Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
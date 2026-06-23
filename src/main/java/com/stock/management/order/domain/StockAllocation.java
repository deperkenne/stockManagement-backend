package com.stock.management.order.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "stock_allocations")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StockAllocation {

    @EmbeddedId
    private AllocationId id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "line_item_id", referencedColumnName = "id", nullable = false)
    private LineItem lineItem;

    @Column(name = "location_id", nullable = false)
    private String locationId;

    // DDD: association par ID vers l'agrégat Sku — pas de FK JPA, lookup via SkuService
    @Column(name = "sku_id", nullable = false, updatable = false)
    private UUID skuId;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "allocated_qty", nullable = false))
    private Quantity allocatedQty;

    @Column(name = "allocated_at", nullable = false)
    private Instant allocatedAt;

    public static StockAllocation create(LineItem lineItem, String locationId, Quantity allocatedQty, UUID skuId) {
        StockAllocation allocation = new StockAllocation();
        allocation.id = AllocationId.generate();
        allocation.lineItem = lineItem;
        allocation.locationId = locationId;
        allocation.skuId = skuId;
        allocation.allocatedQty = allocatedQty;
        allocation.allocatedAt = Instant.now();
        return allocation;
    }
}
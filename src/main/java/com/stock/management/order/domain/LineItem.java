package com.stock.management.order.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "line_items")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LineItem {

    @EmbeddedId
    private LineItemId id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id", referencedColumnName = "id", nullable = false)
    private CustomerOrder customerOrder;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "product_nr", nullable = false))
    private ProductNr productNr;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "requested_qty", nullable = false))
    private Quantity requestedQty;

    @Embedded
    @AttributeOverride(name = "value", column = @Column(name = "allocated_qty", nullable = false))
    private Quantity allocatedQty;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private LineItemStatus status;

    @OneToMany(mappedBy = "lineItem", cascade = CascadeType.ALL, orphanRemoval = true) // mettons en lazy pour eviter de le charger pour question de performence
    private List<StockAllocation> allocations = new ArrayList<>();

    static LineItem create(CustomerOrder order, ProductNr productNr, Quantity requestedQty, BigDecimal unitPrice) {
        LineItem li = new LineItem();
        li.id = LineItemId.generate();
        li.customerOrder = order;
        li.productNr = productNr;
        li.requestedQty = requestedQty;
        li.allocatedQty = Quantity.zero();
        li.unitPrice = unitPrice;
        li.status = LineItemStatus.PENDING;
        return li;
    }

    public void addAllocation(String locationId, Quantity qty, UUID skuId) {
        StockAllocation allocation = StockAllocation.create(this, locationId, qty, skuId);
        allocations.add(allocation);
        allocatedQty = allocatedQty.add(qty);
        status = allocatedQty.isFullyAllocated(requestedQty)
                ? LineItemStatus.FULLY_ALLOCATED
                : LineItemStatus.PARTIALLY_ALLOCATED;
    }

    public boolean isCancellable() {
        return status != LineItemStatus.CANCELLED;
    }

    public void cancel() {
        if (this.status == LineItemStatus.CANCELLED) return;
        this.status = LineItemStatus.CANCELLED;
    }
}

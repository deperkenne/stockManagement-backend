package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDeallocatedEvent {

    private String eventId;
    private String orderId;
    private String warehouseId;
    private DeallocationReason reason;
    private List<DeallocatedLine> deallocatedLines;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    public enum DeallocationReason {
        ORDER_CANCELLED,
        REALLOCATION_REQUESTED,
        WAREHOUSE_CHANGE,
        CUSTOMER_REQUEST,
        SYSTEM_CORRECTION
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DeallocatedLine {
        private String sku;
        private int quantityDeallocated;
        private String locationId;
    }
}
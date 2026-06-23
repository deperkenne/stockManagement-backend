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
public class StockAllocatedEvent {

    private String eventId;
    private String orderId;
    private String warehouseId;
    private List<AllocatedLine> allocatedLines;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AllocatedLine {
        private String sku;
        private int quantityAllocated;
        private String locationId;
    }
}
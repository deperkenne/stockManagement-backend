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
public class StockReleasedEvent {

    private String eventId;
    private String orderId;
    private String warehouseId;
    private String releaseReason;
    private List<ReleasedLine> releasedLines;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReleasedLine {
        private String sku;
        private int quantityReleased;
        private String locationId;
    }
}
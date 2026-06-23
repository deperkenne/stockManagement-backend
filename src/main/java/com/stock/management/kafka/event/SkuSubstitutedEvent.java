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
public class SkuSubstitutedEvent {

    private String eventId;
    private String orderId;
    private String warehouseId;
    private List<Substitution> substitutions;
    private boolean customerNotified;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Substitution {
        private String originalSku;
        private String substituteSku;
        private int quantity;
        private String substitutionReason;
    }
}
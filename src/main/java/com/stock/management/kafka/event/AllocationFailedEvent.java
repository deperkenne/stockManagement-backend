package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.CascadeType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AllocationFailedEvent {

    private String eventId;
    private String orderId;
    private String warehouseId;
    private FailureReason failureReason;

    private List<FailedLine> failedLines;
    private int retryCount;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    public enum FailureReason {
        INSUFFICIENT_STOCK,
        PARTIAL_STOCK,
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FailedLine {
        private String productId;
		private String orderId;
        private int requestedQuantity;
        private int availableQuantity;
		private int allocatedQuantity;
		private int shortageQuantity; //miss quantity
    }
}

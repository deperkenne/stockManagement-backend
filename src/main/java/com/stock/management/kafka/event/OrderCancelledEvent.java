package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.stock.management.order.domain.CancellationSource;
import lombok.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelledEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String reason;
    private String cancelledBy;
    private CancellationSource cancellationSource;
    private CancellationScope scope; // FULL_ORDER ou SINGLE_LINE
	private boolean completeDeliveryRequired;

    // Allocations à libérer — transmises au consumer pour éviter une requête DB supplémentaire
    private List<CancelledAllocation> allocations;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    public enum CancellationScope { FULL_ORDER, SINGLE_LINE }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CancelledAllocation {
        private UUID skuId;
        private String locationId;
        private String productNr;
        private int quantity;
    }
}

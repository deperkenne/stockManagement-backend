package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderReceivedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String warehouseId;
    private List<OrderLine> lines;
    private String currency;
    private BigDecimal totalAmount;
    private String priority;
    private boolean completeDeliveryRequired;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderLine {
        private String sku;
		private String orderId;
        private int quantity;
		private BigDecimal unitPrice;
    }
}

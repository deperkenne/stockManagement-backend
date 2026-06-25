package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.stock.management.order.domain.LineItemStatus;
import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@Getter
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
    @Setter
	@Getter
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderLine {
		private String orderLineItemId;
        private String sku;
		private String orderId;
		@Enumerated(EnumType.STRING)
		@Column(name = "status", nullable = false, length = 30)
		private LineItemStatus status;
        private int quantity;
		private BigDecimal unitPrice;
    }
}

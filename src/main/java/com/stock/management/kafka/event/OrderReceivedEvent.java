package com.stock.management.kafka.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.stock.management.order.domain.LineItemStatus;
import com.stock.management.order.domain.Priority;
import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderReceivedEvent {

    private String eventId;
    private String orderId;
    //private String customerId;
   // private String warehouseId;
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
        private Integer quantity;
		private BigDecimal unitPrice;
    }


	public static List<OrderReceivedEvent> sortByPriority(List<OrderReceivedEvent> events) {
		return events.stream()
			.sorted(Comparator.comparingInt(
				(OrderReceivedEvent e) -> priorityOrdinal(e.getPriority())).reversed())
			.collect(Collectors.toList());
	}

	private static int priorityOrdinal(String priority) {
		try {
			return Priority.valueOf(priority).ordinal();
		} catch (IllegalArgumentException e) {
			return Priority.NORMAL.ordinal();
		}
	}
}

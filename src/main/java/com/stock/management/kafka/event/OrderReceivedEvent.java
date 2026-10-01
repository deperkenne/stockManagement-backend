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
import java.util.Set;
import java.util.UUID;
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
			.sorted(
				Comparator.comparingInt((OrderReceivedEvent e) -> priorityOrdinal(e.getPriority()))
					.thenComparing(OrderReceivedEvent::getOccurredAt)
			)
			.collect(Collectors.toList());
	}

	private static int priorityOrdinal(String priority) {
		return parsePriority(priority).ordinal();
	}

	private static Priority parsePriority(String priority) {
		if (priority == null) {
			return Priority.NORMAL;
		}
		try {
			return Priority.valueOf(priority.trim().toUpperCase());
		} catch (IllegalArgumentException e) {
			return Priority.NORMAL;
		}
	}
	public static List<OrderReceivedEvent> newEventsToProcess(List<OrderReceivedEvent>orderReceivedEvents, Set<UUID> alreadyAllocatedIds){
		List<OrderReceivedEvent> filteredOrderReceivedEvent = orderReceivedEvents.stream()
			.filter(event -> {
				boolean isAlreadyProcessed = alreadyAllocatedIds.contains(event.getOrderId());
				return !isAlreadyProcessed;
			})
			.toList();
		// Si aucun nouvel événement n'est à traiter
		if (filteredOrderReceivedEvent.isEmpty()) {
			throw new IllegalStateException("Tous les événements du batch ont déjà été alloués/traités.");
		}
		return filteredOrderReceivedEvent;
	}

	public static List<String> extractAndSortSkuCodes(List<OrderReceivedEvent> events) {
		return events.stream()
			.flatMap(e -> e.getLines().stream())
			.map(OrderReceivedEvent.OrderLine::getSku)
			.distinct()
			.sorted() // Tri alphabétique anti-deadlock
			.collect(Collectors.toList());
	}


	public static List<String> extractAndSortSkuCodes(OrderReceivedEvent event) {
		return event.getLines().stream()
			.map(OrderReceivedEvent.OrderLine::getSku)
			.distinct()
			.sorted() // Tri alphabétique anti-deadlock
			.collect(Collectors.toList());
	}
}

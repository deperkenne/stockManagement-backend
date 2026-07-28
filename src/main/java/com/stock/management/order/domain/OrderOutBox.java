package com.stock.management.order.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "order_outbox")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderOutBox {
	@Id
	private UUID id;

	@Column(nullable = false)
	private String aggregateType; // Ex: "ORDER"

	@Column(nullable = false)
	private String aggregateId;   // Ex: orderId

	@Column(nullable = false)
	private String eventType;     // Ex: "OrderReceivedEvent"

	@Column(columnDefinition = "TEXT", nullable = false)
	private String payload;       // JSON du contenu de l'événement

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private OutboxStatus status;  // PENDING, SENT, FAILED

	private int retryCount;

	private String lastError;

	@Column(nullable = false)
	private LocalDateTime createdAt;

	private LocalDateTime processedAt;

	public enum OutboxStatus {
		PENDING, SENT, FAILED
	}

}

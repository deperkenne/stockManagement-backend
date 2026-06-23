package com.stock.management.allocation;

import com.stock.management.kafka.event.AllocationFailedEvent;
import com.stock.management.order.domain.OrderId;
import jakarta.persistence.*;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class AllocationFailedOrder {

	@EmbeddedId
	private OrderId id; // Réutilisation de ton OrderId (UUID)

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private FailureReason failureReason; // COMPLETE_DELIVERY_FAILED ou PARTIAL_DELIVERY_FAILED

	@Enumerated(EnumType.STRING)
	@Column(nullable = false)
	private ProcessedStatus status; // PENDING, REPLAYED, RESOLVED

	/*@OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
	@JoinColumn(name = "failed_order_id")
	@Builder.Default
	private List<AllocationFailedEvent.FailedLine> lines = new ArrayList<>();*/
	@OneToMany(mappedBy = "allocationFailedOrder", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<FailedLine> failedLines;

	public void addLine(FailedLine line) {
		this.failedLines.add(line);
	}

	public enum FailureReason {
		INSUFFICIENT_STOCK,
		PARTIAL_STOCK,
	}

	public enum ProcessedStatus {
		PENDING,   // En attente de réapprovisionnement
		REPLAYING, // En cours de traitement par le retry
		RESOLVED   // Traité avec succès
	}
}



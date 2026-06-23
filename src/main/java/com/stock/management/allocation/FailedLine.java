package com.stock.management.allocation;

import com.stock.management.order.domain.CustomerOrder;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "allocation_failed_lines")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FailedLine {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "order_id", referencedColumnName = "id", nullable = false)
	private AllocationFailedOrder allocationFailedOrder;
	@Column(nullable = false)
	private String sku;

	@Column(nullable = false)
	private int quantityNeeded;

	@Column(nullable = false)
	private int quantityAllocated; // 0 pour completeDelivery, > 0 pour partial

	@Column(nullable = false)
	private int stockAvailableAtFailure;

	@Column(nullable = false)
	private int shortageQuantity;


}


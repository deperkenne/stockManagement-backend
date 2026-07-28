package com.stock.management.order;

import com.stock.management.order.domain.OrderOutBox;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OrderOutboxRepository extends JpaRepository<OrderOutBox, UUID> {

	// Récupère les événements en attente ou en échec avec un nombre de tentatives max
	List<OrderOutBox> findTop50ByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(
		List<OrderOutBox.OutboxStatus> statuses,
		int maxRetries
	);
}

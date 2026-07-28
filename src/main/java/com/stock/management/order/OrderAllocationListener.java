package com.stock.management.order;

import com.stock.management.allocation.AllocationService;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.domain.CustomerOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderAllocationListener {
	private final AllocationService allocationService;

	// BEFORE_COMMIT ou AFTER_COMMIT sécurise le flux
	//Ce composant écoute un événement et exécute l'allocation uniquement si la commande
	// a été sauvegardée avec succès en base de données, le tout de manière non bloquante pour l'utilisateur.
	@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
	@Async // Permet de libérer immédiatement le thread de l'utilisateur (Optionnel mais recommandé)
	public void handleOrderCreated(List<OrderReceivedEvent> orderReceivedEvents) {

		try {
			allocationService.allocate(orderReceivedEvents);
		} catch (Exception ex) {
			// On extrait proprement la liste des IDs pour le log
			List<UUID> orderIds = orderReceivedEvents.stream()
				.map(order -> UUID.fromString(order.getOrderId()))
				.toList(); // Ou .collect(Collectors.toList()) si tu es sur une version de Java < 16

			log.error("[ORDER] Échec de l'allocation automatique en lot pour les commandes : {} | Cause : {}",
				orderIds, ex.getMessage(), ex);

		}
	}
}

package com.stock.management.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.domain.OrderOutBox;
import com.stock.management.order.OrderOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {
	private final OrderOutboxRepository outboxRepository;
	//private final KafkaTemplate<String, String> kafkaTemplate;
	// Spring injecte automatiquement le Bean ObjectMapper de l'application ici !
	private final ObjectMapper objectMapper;
	private final KafkaEventPublisher kafkaEventPublisher;
	private static final int MAX_RETRIES = 5;


	// S'exécute toutes les 5 secondes
	@Scheduled(fixedDelay = 5000)
	public void processOutboxEvents() {
		log.info("[OUTBOX] start schedule");
		// Récupère les messages PENDING ou FAILED ayant moins de 5 essais
		List<OrderOutBox.OutboxStatus> eligibleStatuses = List.of(
			OrderOutBox.OutboxStatus.PENDING,
			OrderOutBox.OutboxStatus.FAILED
		);

		List<OrderOutBox> pendingEvents = outboxRepository
			.findTop50ByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(eligibleStatuses, MAX_RETRIES);

		if (pendingEvents.isEmpty()) {
			log.info("[OUTBOX] aucun message pour le moment ");
			return;
		}

		log.info("[OUTBOX] Dépilage de {} événement(s) en attente...", pendingEvents.size());

		for (OrderOutBox event : pendingEvents) {
			publishEvent(event);
		}
	}

	private void publishEvent(OrderOutBox orderOutBox) {
		String topic = "order.received";

		try {
			// 1. Reconstruire l'objet Java métier à partir du JSON BDD
			OrderReceivedEvent orderReceivedEvent = objectMapper.readValue(
				orderOutBox.getPayload(),
				OrderReceivedEvent.class
			);

			// // Envoi synchrone vers Kafka (.get() attend l'acquittement du Broker)
			kafkaEventPublisher.publishOrderReceived(orderReceivedEvent);

			//  SUCCÈS : Mise à jour du statut
			orderOutBox.setStatus(OrderOutBox.OutboxStatus.SENT);
			orderOutBox.setProcessedAt(LocalDateTime.now());
			outboxRepository.save(orderOutBox);

			log.info("[OUTBOX] Événement {} publié avec succès sur Kafka.", orderOutBox.getId());

		} catch (Exception e) {
			// Gestion des erreurs (Broker down, échec de désérialisation, etc.)
			handleFailure(orderOutBox, e.getMessage());
		}
	}

	private void handleFailure(OrderOutBox event, String errorMessage) {
		int nextRetry = event.getRetryCount() + 1;
		event.setRetryCount(nextRetry);
		event.setStatus(OrderOutBox.OutboxStatus.FAILED);
		event.setLastError(errorMessage);

		if (nextRetry >= MAX_RETRIES) {
			log.error("[OUTBOX] Échec critique : L'événement {} a atteint le nombre max de réessais ({}).",
				event.getId(), MAX_RETRIES);
		} else {
			log.warn("[OUTBOX] Échec de l'envoi de l'événement {} (Tentative {}/{}). Motif: {}",
				event.getId(), nextRetry, MAX_RETRIES, errorMessage);
		}

		outboxRepository.save(event);
	}
}

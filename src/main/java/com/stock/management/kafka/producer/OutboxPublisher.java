package com.stock.management.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.domain.OrderOutBox;
import com.stock.management.order.OrderOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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


	// S'exécute toutes les 6 secondes
	@Scheduled(fixedDelay = 6000)
	public void processOutboxEvents() {
		log.info("[OUTBOX] start schedule");
		List<OrderOutBox.OutboxStatus> eligibleStatuses = List.of(
			OrderOutBox.OutboxStatus.PENDING,
			OrderOutBox.OutboxStatus.FAILED
		);

		List<OrderOutBox> pendingEvents = outboxRepository
			.findTop50ByStatusInAndRetryCountLessThanOrderByCreatedAtAsc(eligibleStatuses, MAX_RETRIES);

		if (pendingEvents.isEmpty()) {
			log.info("\u001B[36m[OUTBOX]\u001B[0m Aucun message pour le moment");
			return;
		}

		log.info("\u001B[36m[OUTBOX]\u001B[0m Dépilage de {} événement(s) en attente...", pendingEvents.size());

		// Phase 1 : soumettre TOUS les envois Kafka d'abord, sans attendre entre chaque
		// (préserve le batching : les send() s'enchaînent vite, sous linger.ms)
		Map<OrderOutBox, CompletableFuture<SendResult<String, Object>>> futuresByEvent = new LinkedHashMap<>();

		for (OrderOutBox event : pendingEvents) {
			CompletableFuture<SendResult<String, Object>> future = submitEvent(event);
			if (future != null) {
				futuresByEvent.put(event, future);
			}
		}

		// Phase 2 : attendre chaque confirmation, APRÈS que tous les send() aient été soumis
		futuresByEvent.forEach(this::awaitAndFinalize);
	}

	private CompletableFuture<SendResult<String, Object>> submitEvent(OrderOutBox orderOutBox) {
		try {
			OrderReceivedEvent orderReceivedEvent = objectMapper.readValue(
				orderOutBox.getPayload(),
				OrderReceivedEvent.class
			);

			log.info("Ordre JUSTE APRÈS readValue (outbox): {}",
				orderReceivedEvent.getLines().stream()
					.map(OrderReceivedEvent.OrderLine::getSku)
					.toList());

			byte[] serialized = objectMapper.writeValueAsBytes(orderReceivedEvent);
			log.info("[KAFKA-SIZE] Taille du message sérialisé : {} octets", serialized.length);

			return kafkaEventPublisher.publishOrderReceived(orderReceivedEvent);

		} catch (Exception e) {
			// échec AVANT même l'envoi Kafka (ex: désérialisation) -> échec immédiat
			handleFailure(orderOutBox, e.getMessage());
			return null;
		}
	}

	private void awaitAndFinalize(OrderOutBox orderOutBox, CompletableFuture<SendResult<String, Object>> future) {
		try {
			SendResult<String, Object> result = future.get(10, TimeUnit.SECONDS);
			RecordMetadata metadata = result.getRecordMetadata();

			orderOutBox.changeStatusToSent(OrderOutBox.OutboxStatus.SENT);
			orderOutBox.updateTime();
			outboxRepository.save(orderOutBox);

			log.info("\u001B[32m[OUTBOX]\u001B[0m Événement {} publié avec succès (partition={}, offset={}).",
				orderOutBox.getId(), metadata.partition(), metadata.offset());

		} catch (Exception e) {
			handleFailure(orderOutBox, e.getMessage());
		}
	}

	private void handleFailure(OrderOutBox event, String errorMessage) {
		int nextRetry = event.getRetryCount() + 1;
		event.updateRetryCount(nextRetry);
		event.changeStatusToFailed(OrderOutBox.OutboxStatus.FAILED);
		event.changeLastError(errorMessage);

		if (nextRetry >= MAX_RETRIES) {
			log.error("\u001B[31m[OUTBOX] Échec critique : L'événement {} a atteint le nombre max de réessais ({}).\u001B[0m",
				event.getId(), MAX_RETRIES);
		} else {
			log.warn("[OUTBOX] Échec de l'envoi de l'événement {} (Tentative {}/{}). Motif: {}",
				event.getId(), nextRetry, MAX_RETRIES, errorMessage);
		}

		//outboxRepository.save(event);
	}
}

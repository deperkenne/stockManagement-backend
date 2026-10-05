package com.stock.management.kafka.consumer;

import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.kafka.handler.KafkaEventHandler;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.stock.management.kafka.config.KafkaTopics.DLT_TOPIC;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("test")
public class OrderEventConsumerTest {
	private final KafkaEventHandler eventHandler;
	private final KafkaEventPublisher kafkaEventPublisher;


	// Batch listener : reçoit jusqu'à max.poll.records messages en un seul appel.
	// AllocationService traite tout le lot avec 1 seul appel DB (findAvailableSkusForAllocationWithLock).
	@KafkaListener(
		id = "orderReceivedTestListener",
		topics = KafkaTopics.ORDER_RECEIVED_TEST03,
		groupId = "${spring.kafka.consumer.test-group-id01}",
		containerFactory = "batchKafkaListenerContainerFactory"
	)

	// List<OrderReceivedEvent> orderReceivedEvents (grâce à containerFactory = "batchKafkaListenerContainerFactory") reçoit tous les messages ramenés par un seul poll() — s'il y a eu 30 messages disponibles au moment du poll (dans la limite de max-poll-records: 50), les 30 arrivent groupés dans cette même liste,
	// en un seul appel de méthode. C'est exactement ce mécanisme qui permet à
	public void onOrderReceivedTest(
		@Payload List<OrderReceivedEvent> orderReceivedEvents,
		@Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
		@Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
		@Header(KafkaHeaders.OFFSET) List<Long> offsets,
		Acknowledgment ack) throws Exception {

		log.info("[KAFKA] Received batch of {} orders — first: topic={} partition={} offset={}",
			orderReceivedEvents.size(), topics.get(0), partitions.get(0), offsets.get(0));
		try {

			// ← AJOUTE CE BLOC ICI, avant tout traitement
			orderReceivedEvents.forEach(event ->
				log.info("[ORDER-LINES-CHECK] orderId={} lines={}",
					event.getOrderId(),
					event.getLines().stream()
						.map(OrderReceivedEvent.OrderLine::getSku)
						.toList())
			);
			// 1. Tentative sur le lot complet (Batch)
			eventHandler.handleOrderReceivedBatch(orderReceivedEvents);
			log.info("commit sucessfull...................");


		} catch (Exception ex) {
			log.error("[KAFKA] Échec du batch (taille={}). Début du fallback unitaire avec retries. Cause: {}",
				orderReceivedEvents.size(), ex.getMessage());
			//throw new Exception(ex.getMessage());
		}
		ack.acknowledge();
	}


	private void executeWithRetry(Runnable action, int maxAttempts, long backoffMs) throws Exception {
		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			try {
				action.run();
				return; // Succès : sortie immédiate
			} catch (Exception ex) {
				if (attempt == maxAttempts) throw ex; // Dernier échec : on propage l'erreur vers le catch du DLT
				Thread.sleep(backoffMs);
			}
		}
	}

	/*
	@KafkaListener(topics = "order.received-dlt", groupId = "order-dlt-group")
	public void processDltMessages( @Payload List<OrderReceivedEvent> orderReceivedEvents, Acknowledgment ack) {
		try {
			log.warn("[DLT PROCESS] Traitement/Audit du message en échec : key={}");
			// Sauvegarde dans une table de base de données d'audit ou notification Slack/Email
		} catch (Exception ex) {
			log.error("[DLT ERROR] Échec de traitement du DLT. Le message est ignoré pour éviter la boucle.", ex);
		} finally {

			ack.acknowledge();
		}
	}

	 */

}

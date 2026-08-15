package com.stock.management.kafka.consumer;

import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.kafka.event.*;
import com.stock.management.kafka.handler.KafkaEventHandler;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.domain.CustomerOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
public class OrderEventConsumer {

    private final KafkaEventHandler eventHandler;
	private final KafkaEventPublisher kafkaEventPublisher;



    // Batch listener : reçoit jusqu'à max.poll.records messages en un seul appel.
    // AllocationService traite tout le lot avec 1 seul appel DB (findAvailableSkusForAllocationWithLock).
    @KafkaListener(
            topics = KafkaTopics.ORDER_RECEIVED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "batchKafkaListenerContainerFactory"
    )

	// List<OrderReceivedEvent> orderReceivedEvents (grâce à containerFactory = "batchKafkaListenerContainerFactory") reçoit tous les messages ramenés par un seul poll() — s'il y a eu 30 messages disponibles au moment du poll (dans la limite de max-poll-records: 50), les 30 arrivent groupés dans cette même liste,
	// en un seul appel de méthode. C'est exactement ce mécanisme qui permet à
    public void onOrderReceived(
            @Payload List<OrderReceivedEvent> orderReceivedEvents,
            @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment ack) {

		log.info("[KAFKA] Received batch of {} orders — first: topic={} partition={} offset={}",
			orderReceivedEvents.size(), topics.get(0), partitions.get(0), offsets.get(0));
		try {
			// 1. Tentative sur le lot complet (Batch)
			eventHandler.handleOrderReceivedBatch(orderReceivedEvents);

		} catch (Exception ex) {
			log.warn("[KAFKA] Échec du batch (taille={}). Début du fallback unitaire avec retries. Cause: {}",
				orderReceivedEvents.size(), ex.getMessage());

			// DANS VOTRE LISTENER (REPLI BATCH) ──────────────────────────────────────
			for (int i = 0; i < orderReceivedEvents.size(); i++) {
				OrderReceivedEvent event = orderReceivedEvents.get(i);
				try {
					// Exécute 3 tentatives espacées de 500ms
					executeWithRetry(() -> eventHandler.handleOrderReceived(event), 3, 500);
				} catch (Exception singleEx) {
					log.error("[DLT ISOLATION] Échec définitif pour la commande {} (Offset: {}). Cause: {}",
						event.getOrderId(), offsets.get(i), ex.getMessage());

					kafkaEventPublisher.sendToDlt(DLT_TOPIC, event.getOrderId().toString(), event, ex);
				}
			}
			//  Validation explicite de l'offset une fois TOUT le batch traité (hors de la boucle)
			ack.acknowledge();
		}
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

}





	/*
    @KafkaListener(
            topics = KafkaTopics.ORDER_CANCELLED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCancelled(
            @Payload OrderCancelledEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[KAFKA] Received {} topic={} partition={} offset={}", event.getClass().getSimpleName(), topic, partition, offset);
        try {
            eventHandler.handleOrderCancelled(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("[KAFKA] Processing failed for orderId={} : {}", event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = KafkaTopics.STOCK_RELEASED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onStockReleased(
            @Payload StockReleasedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[KAFKA] Received {} topic={} partition={} offset={}", event.getClass().getSimpleName(), topic, partition, offset);
        try {
            eventHandler.handleStockReleased(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("[KAFKA] Processing failed for orderId={} : {}", event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = KafkaTopics.SKU_CORRECTED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onSkuCorrected(
            @Payload SkuCorrectedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[KAFKA] Received {} topic={} partition={} offset={}", event.getClass().getSimpleName(), topic, partition, offset);
        try {
            eventHandler.handleSkuCorrected(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("[KAFKA] Processing failed for orderId={} : {}", event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = KafkaTopics.ALLOCATION_FAILED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onAllocationFailed(
            @Payload AllocationFailedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[KAFKA] Received {} topic={} partition={} offset={}", event.getClass().getSimpleName(), topic, partition, offset);
        try {
            eventHandler.handleAllocationFailed(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("[KAFKA] Processing failed for orderId={} : {}", event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = KafkaTopics.SKU_SUBSTITUTED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onSkuSubstituted(
            @Payload SkuSubstitutedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[KAFKA] Received {} topic={} partition={} offset={}", event.getClass().getSimpleName(), topic, partition, offset);
        try {
            eventHandler.handleSkuSubstituted(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("[KAFKA] Processing failed for orderId={} : {}", event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = KafkaTopics.ORDER_DEALLOCATED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderDeallocated(
            @Payload OrderDeallocatedEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        log.info("[KAFKA] Received {} topic={} partition={} offset={}", event.getClass().getSimpleName(), topic, partition, offset);
        try {
            eventHandler.handleOrderDeallocated(event);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("[KAFKA] Processing failed for orderId={} : {}", event.getOrderId(), ex.getMessage(), ex);
            throw ex;
        }
    }

	 */


package com.stock.management.kafka.consumer;

import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.kafka.event.*;
import com.stock.management.kafka.handler.KafkaEventHandler;
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

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final KafkaEventHandler eventHandler;

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
			log.info("[KAFKA] print orderId {} ", orderReceivedEvents.get(0).getOrderId().toString());
            eventHandler.handleOrderReceivedBatch(orderReceivedEvents);


			//Ce que fait réellement ack.acknowledge() en mode batch
			//
			//Avec ack-mode: manual_immediate + listener batch, ack.acknowledge() (ligne 43) ne commit pas les offsets un par un.
			// Il fait un seul commit atomique qui dit au broker :
			// "pour cette partition, tout ce qui est ≤ au dernier offset de ce batch est traité".
			// Concrètement, si le batch contient les offsets [10, 11, 12] de la partition 0,
			// un seul appel ack.acknowledge() commit 13 (le prochain offset à lire) pour cette partition — pas trois commits séparés 10, 11, 12.
			//
			//Pourquoi ça explique le comportement "tout ou rien" observé
			//
			//C'est exactement pour ça qu'un seul message défaillant dans le ba
			//Et ack.acknowledge() (ligne 43) commite tous les offsets du batch d'un coup — c'est pour ça qu'un seul message qui échoue fait échouer (et rejouer) tout le batch entier au prochain restart,
			//puisque ack.acknowledge() n'est jamais atteint si handleOrderReceivedBatch lève une exception.

            ack.acknowledge(); // lorsqu'il est atteind on passe a l'offset suivant
        } catch (Exception ex) {
            log.error("[KAFKA] Batch processing failed size={} : {}", orderReceivedEvents.size(), ex.getMessage());
			// 🎯 OPTION A : Avancer l'offset de la partition au dernier offset du batch + 1 (pour sauter tout le batch bloquant)
			ack.acknowledge();
            throw ex;
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
}

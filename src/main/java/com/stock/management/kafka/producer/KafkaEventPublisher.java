package com.stock.management.kafka.producer;

import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.kafka.event.*;
import com.stock.management.order.domain.CustomerOrder;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;



	public  void sendToDlt(String dltTopic, String key, Object payload, Throwable exception) {
		// 1. Création des en-têtes d'erreur standards (pour garder la traçabilité)
		ProducerRecord<String, Object> record = new ProducerRecord<>(dltTopic, key, payload);

		record.headers().add("kafka_dlt-exception-message",
			exception.getMessage() != null ? exception.getMessage().getBytes(StandardCharsets.UTF_8) : new byte[0]);
		record.headers().add("kafka_dlt-exception-stacktrace",
			getStackTraceAsString(exception).getBytes(StandardCharsets.UTF_8));
		record.headers().add("kafka_dlt-original-timestamp",
			String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

		// 2. Envoi asynchrone vers le topic DLT
		kafkaTemplate.send(record).whenComplete((result, ex) -> {
			if (ex != null) {
				log.error("[DLT CRITICAL ERROR] Échec d'envoi vers la DLT {} pour la clé {}: {}",
					dltTopic, key, ex.getMessage(), ex);
			} else {
				log.info("[DLT SUCCESS] Message isolé avec succès dans {} (Partition: {}, Offset: {})",
					dltTopic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
			}
		});
	}

	private String getStackTraceAsString(Throwable throwable) {
		StringWriter sw = new StringWriter();
		throwable.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}


	/*
    public void publishStockAllocated(StockAllocatedEvent event) {
        send(KafkaTopics.STOCK_ALLOCATED, event.getOrderId(), event);
    }

    public void publishOrderCancelled(OrderCancelledEvent event) {
        send(KafkaTopics.ORDER_CANCELLED, event.getOrderId(), event);
    }

    public void publishStockReleased(StockReleasedEvent event) {
        send(KafkaTopics.STOCK_RELEASED, event.getOrderId(), event);
    }

    public void publishSkuCorrected(SkuCorrectedEvent event) {
        send(KafkaTopics.SKU_CORRECTED, event.getOrderId(), event);
    }

    public void publishAllocationFailed(AllocationFailedEvent event) {
        send(KafkaTopics.ALLOCATION_FAILED, event.getOrderId(), event);
    }

    public void publishSkuSubstituted(SkuSubstitutedEvent event) {
        send(KafkaTopics.SKU_SUBSTITUTED, event.getOrderId(), event);
    }

    public void publishOrderDeallocated(OrderDeallocatedEvent event) {
        send(KafkaTopics.ORDER_DEALLOCATED, event.getOrderId(), event);
    }


	public void publishOrderINPROGRESS(List<OrderReceivedEvent> events){
		sendOrderEvents(KafkaTopics.ORDER_INPROGRESS, events);
	}


	private void sendOrderEvents(String topic, List<OrderReceivedEvent> events) {
		if (events == null || events.isEmpty()) {
			log.debug("No events to send to Kafka for topic [{}]. Skipping.", topic);
			return;
		}
		events.forEach(event -> send(topic, event.getOrderId(),event));
	}

	private List<UUID> orderIds (List<OrderReceivedEvent> events){
		List<UUID> ids = events.stream()
			.map(OrderReceivedEvent::getOrderId)
			.map(UUID::fromString)
			.toList();
		return ids;
	}

	 */
    // Avec ce @PostConstruct, le coût de 292ms est payé au démarrage du contexte Spring
	// (donc avant même que votre premier test ne s'exécute)
	@PostConstruct
	public void warmUp() {
		try {
			long start = System.currentTimeMillis();

			kafkaTemplate.send("_internal.warmup", "warmup-key", "warmup-payload")
				.get(10, TimeUnit.SECONDS);

			long elapsed = System.currentTimeMillis() - start;
			log.info("[KAFKA] Producer réellement préchauffé en {} ms", elapsed);
		} catch (Exception e) {
			log.warn("[KAFKA] Échec du préchauffage du producer : {}", e.getMessage());
		}
	}




	public CompletableFuture<SendResult<String, Object>> publishOrderReceived(OrderReceivedEvent event) {
		return send(KafkaTopics.ORDER_RECEIVED_TEST03, event.getOrderId(), event);
	}


	private CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object payload) {
		CompletableFuture<SendResult<String, Object>> future =
			kafkaTemplate.send(topic, key, payload);

		future.whenComplete((result, ex) -> {
			if (ex != null) {
				log.error("[KAFKA] Failed to publish to topic={} key={} : {}", topic, key, ex.getMessage(), ex);
			} else {
				log.debug("[KAFKA] Published topic={} key={} partition={} offset={}",
					topic, key,
					result.getRecordMetadata().partition(),
					result.getRecordMetadata().offset());
			}
		});

		return future;  // on expose le Future au lieu de le garder privé
	}


}

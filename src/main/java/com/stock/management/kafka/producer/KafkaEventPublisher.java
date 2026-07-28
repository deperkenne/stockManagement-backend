package com.stock.management.kafka.producer;

import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.kafka.event.*;
import com.stock.management.order.domain.CustomerOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

	public void publishOrderReceived(OrderReceivedEvent event) {
		send(KafkaTopics.ORDER_RECEIVED, event.getOrderId(), event);
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
    private void send(String topic, String key, Object payload) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, key, payload);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA] Failed to publish to topic={} key={} : {}",
                        topic, key, ex.getMessage(), ex);
            } else {
                log.debug("[KAFKA] Published topic={} key={} partition={} offset={}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}

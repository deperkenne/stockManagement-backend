package com.stock.management.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeadLetterConsumer {

    @KafkaListener(
            topicPattern = ".*\\.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDeadLetter(ConsumerRecord<String, Object> record) {
        log.error("[DLT] Dead-letter message: topic={} key={} partition={} offset={} payload={}",
                record.topic(),
                record.key(),
                record.partition(),
                record.offset(),
                record.value());
        // TODO: persist to dead-letter store / alert ops team
    }
}
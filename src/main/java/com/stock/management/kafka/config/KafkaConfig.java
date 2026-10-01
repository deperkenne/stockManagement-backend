package com.stock.management.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;


import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.topic.partitions:3}")
    private int partitions;

    @Value("${spring.kafka.topic.replication-factor:1}")
    private short replicationFactor;

    // ─── ObjectMapper ────────────────────────────────────────────────────────────

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // ─── Listener container ───────────────────────────────────────────────────────
    // ProducerFactory, ConsumerFactory et KafkaTemplate sont auto-configurés par
    // Spring Boot via application.yml (value-serializer / value-deserializer).

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // Factory dédiée au batch — utilisée uniquement par ORDER_RECEIVED
    // max.poll.records contrôle la taille du batch (configuré dans application.yml)
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> batchKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // ─── Error handling / DLT ────────────────────────────────────────────────────

	@Bean
	public DefaultErrorHandler errorHandler(KafkaTemplate<?, ?> kafkaTemplate) {
		// Exponential backoff: 1s initial, 2x multiplier, max 10s
		ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
		backOff.setMaxElapsedTime(10_000L);


		//  Option A : Retourner un TopicPartition sans spécifier la partition (constructeur à 1 argument)
		// En ne passant pas le 2ème argument 'int', Kafka utilisera le partitionnement par défaut (Key Hash / Round-Robin)
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
			kafkaTemplate,
			(record, ex) -> {
				String dltTopic = record.topic() + "-dlt";

				//  Récupération dynamique du nombre de partitions via le KafkaTemplate :
				List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(dltTopic);
				int partitionCount = partitions != null ? partitions.size() : 1;

				// Calcul du hash de la clé (Exemple explicite)
				int targetPartition = 0;
				if (record.key() != null) {
					targetPartition = Math.abs(record.key().hashCode()) % partitionCount;
				}

				return new TopicPartition(dltTopic, targetPartition);
			}
		);

		DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

		// 3. Exceptions NON-RETRYABLE (Redirection immédiate vers le DLT)
		handler.addNotRetryableExceptions(
			IllegalArgumentException.class,
			IllegalStateException.class,
			NullPointerException.class,
			InvocationTargetException.class // ⚠️ Indispensable en raison du dispatch par réflexion
		);

		// 4. (Optionnel) Listener de logs à chaque tentative de Retry
		handler.setRetryListeners((record, ex, attempt) ->
			log.warn("\u001B[34m[KAFKA RETRY]\u001B[0m Échec tentative #{} pour le message key={} sur topic={}. Cause: {}",
				attempt, record.key(), record.topic(), ex.getMessage())

		);

		return handler;
	}

    // ─── Topics ──────────────────────────────────────────────────────────────────

    @Bean
    public NewTopic orderReceivedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_RECEIVED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")      // 7 days
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

	@Bean
	public NewTopic orderReceivedTestTopic() {
		return TopicBuilder.name(KafkaTopics.ORDER_RECEIVED_TEST03)
			.partitions(1)
			.replicas(replicationFactor)
			.config("retention.ms", "1")
			.config("segment.ms", "1")       // force un roulement de segment toutes les 100ms
			.config("cleanup.policy", "delete")
			.config("compression.type", "snappy")
			.build();
	}



	/*

	@Bean NewTopic orderChangeStatus(){

		return TopicBuilder.name(KafkaTopics.ORDER_RECEIVED)
			.partitions(partitions)
			.replicas(replicationFactor)
			.config("retention.ms", "604800000")      // 7 days
			.config("cleanup.policy", "delete")
			.config("compression.type", "snappy")
			.build();
	}

    @Bean
    public NewTopic stockAllocatedTopic() {
        return TopicBuilder.name(KafkaTopics.STOCK_ALLOCATED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic orderCancelledTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_CANCELLED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic stockReleasedTopic() {
        return TopicBuilder.name(KafkaTopics.STOCK_RELEASED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic skuCorrectedTopic() {
        return TopicBuilder.name(KafkaTopics.SKU_CORRECTED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "2592000000")     // 30 days — audit trail
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic allocationFailedTopic() {
        return TopicBuilder.name(KafkaTopics.ALLOCATION_FAILED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic skuSubstitutedTopic() {
        return TopicBuilder.name(KafkaTopics.SKU_SUBSTITUTED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "2592000000")
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic orderDeallocatedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_DEALLOCATED)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000")
                .config("cleanup.policy", "delete")
                .config("compression.type", "snappy")
                .build();
    }

    // ─── Dead-letter topics (auto-created by DeadLetterPublishingRecoverer) ──────
    // Naming convention: <original-topic>.DLT

    @Bean
    public NewTopic orderReceivedDlt() {
        return TopicBuilder.name(KafkaTopics.ORDER_RECEIVED + ".DLT")
                .partitions(1)
                .replicas(replicationFactor)
                .config("retention.ms", "2592000000")
                .build();
    }

    @Bean
    public NewTopic allocationFailedDlt() {
        return TopicBuilder.name(KafkaTopics.ALLOCATION_FAILED + ".DLT")
                .partitions(1)
                .replicas(replicationFactor)
                .config("retention.ms", "2592000000")
                .build();
    }

	 */
}

package com.stock.management.domain;

import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.order.OrderOutboxRepository;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.LineItemRequest;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.domain.Sku;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HandlePriorityBasedAllocationEndToEndIT {
	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	@Autowired
	private OrderService orderService;
	@Autowired
	private OrderRepository orderRepository;
	@Autowired
	private SkuRepository skuRepository;
	@Autowired
	private OrderOutboxRepository outboxRepository;
	@Autowired
	private AllocationItemRepository allocationItemRepository;


	@Autowired
	private KafkaAdmin kafkaAdmin;

	private static final String TEST_GROUP_ID = "stock-management-group-test01-" + UUID.randomUUID();

	@DynamicPropertySource
	static void kafkaProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.consumer.test-group-id01", () -> TEST_GROUP_ID);
	}

	@BeforeAll
	void purgeTestTopic() {
		try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
			Map<String, Object> endOffsets = admin.listOffsets(
					Map.of(new TopicPartition(KafkaTopics.ORDER_RECEIVED_TEST03, 0), OffsetSpec.latest())
				).all().get(10, TimeUnit.SECONDS)
				.entrySet().stream()
				.collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().offset()));

			long latestOffset = (long) endOffsets.get(KafkaTopics.ORDER_RECEIVED_TEST03 + "-0");

			admin.deleteRecords(Map.of(
				new TopicPartition(KafkaTopics.ORDER_RECEIVED_TEST03, 0),
				RecordsToDelete.beforeOffset(latestOffset)
			)).all().get(10, TimeUnit.SECONDS);

			log.info("[TEST] Topic {} purgé jusqu'à l'offset {}", KafkaTopics.ORDER_RECEIVED_TEST03, latestOffset);
		} catch (Exception e) {
			log.warn("[TEST] Purge du topic {} échouée, poursuite sans purge. Cause: {}",
				KafkaTopics.ORDER_RECEIVED_TEST03, e.getMessage());
		}
	}

	@AfterEach
	void cleanUp() {
		outboxRepository.deleteAll();
		orderRepository.deleteAll();
		skuRepository.deleteAll();
		allocationItemRepository.deleteAll();
	}

	private LineItemRequest createLineItemRequest(String productNr, int quantity, BigDecimal unitPrice) {
		return new LineItemRequest(productNr, quantity, unitPrice);
	}

	private List<Sku> createAndSaveSku() {
		return skuRepository.saveAll(List.of(
			Sku.create(new ProductNr("PROD01"), new Quantity(90), "WA-01"),
			Sku.create(new ProductNr("PROD02"), new Quantity(80), "WA-02"),
			Sku.create(new ProductNr("PROD03"), new Quantity(20), "WA-03")
		));
	}


	private record OrderLineRequest(
		String orderLineItemId,
		String sku,
		int quantity,
		BigDecimal unitPrice,
		LineItemStatus status
	) {
	}


	/**
	 * Variante END-TO-END : passe par le vrai chemin production, outbox inclus.
	 * OutboxPublisher.processOutboxEvents() tourne toutes les 3s (@Scheduled fixedDelay=3000)
	 * et traite jusqu'à 50 entrées PENDING/FAILED par cycle — largement suffisant pour
	 * absorber nos 3 commandes de test en UN SEUL cycle, pas besoin d'attendre plusieurs
	 * passages. Le timeout de 20s reste confortable (~6 cycles) sans être excessif.
	 * Utile pour valider le pipeline complet ; le test ci-dessus reste préférable
	 * pour un feedback rapide sur le listener seul, isolé de l'outbox.
	 */
	@RepeatedTest(value = 10, name = "Run {currentRepetition}/{totalRepetitions}")
	@DisplayName("End-to-end : receiveOrder() → outbox → poller (3s) → Kafka → listener → allocation, sans contourner l'outbox")
	void fullPipeline_fromReceiveOrderThroughOutboxToAllocation_eventuallyAllocatesAllOrders() throws InterruptedException, ExecutionException, TimeoutException {
		// GIVEN : stock préexistant pour 3 produits distincts
		// create and save Sku
		// create and save Sku
		createAndSaveSku();

		// saved tree Customer order parelelle
		LineItemRequest item01 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item02 = new LineItemRequest("PROD02", 60, new BigDecimal("10.00"));
		LineItemRequest item03 = new LineItemRequest("PROD01", 50, new BigDecimal("10.00"));
		LineItemRequest item04 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item05 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item06 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item07 = createLineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item08 = createLineItemRequest("PROD02", 40, new BigDecimal("10.00"));

		CreateOrderRequest createOrderRequest01 = new CreateOrderRequest(
			Priority.LOW, false, "EUR", List.of(item01, item02));

		CreateOrderRequest createOrderRequest04 = new CreateOrderRequest(
			Priority.NORMAL, false, "EUR", List.of(item07, item08));

		CreateOrderRequest createOrderRequest02 = new CreateOrderRequest(
			Priority.HIGH, true, "EUR", List.of(item03, item04));

		CreateOrderRequest createOrderRequest03 = new CreateOrderRequest(
			Priority.NORMAL, true, "EUR", List.of(item05, item06));


		String lowOrderId = orderService.receiveOrder(createOrderRequest01).orderId();
		String normalOrderId2 = orderService.receiveOrder(createOrderRequest04).orderId();
		String highOrderId = orderService.receiveOrder(createOrderRequest02).orderId();
		String normalOrderId = orderService.receiveOrder(createOrderRequest03).orderId();


		List<String> orderIds = List.of(lowOrderId, highOrderId, normalOrderId,normalOrderId2);

		// THEN : on attend que TOUTES les commandes soient sorties de l'état PENDING
		orderIds.forEach(id ->
			await()
				.atMost(15, TimeUnit.SECONDS)
				.pollInterval(200, TimeUnit.MILLISECONDS)
				.untilAsserted(() -> {
					CustomerOrder order = orderRepository.findById(new OrderId(UUID.fromString(id))).orElseThrow();
					assertThat(order.getStatus()).isIn(
						OrderStatus.ALLOCATION_FAILED,
						OrderStatus.FULLY_ALLOCATED,
						OrderStatus.PARTIALLY_ALLOCATED
					);
				})
		);

		// Vérification complémentaire : l'outbox doit être vidée (confirme que Kafka a bien reçu les messages)
		await()
			.atMost(15, TimeUnit.SECONDS)   // aligné sur le même délai, > au fixedDelay du scheduler (6s)
			.pollInterval(200, TimeUnit.MILLISECONDS)
			.untilAsserted(() -> {
				assertThat(outboxRepository.findAll())
					.as("Une fois publiées avec succès par le poller, les entrées outbox doivent être SENT")
					.filteredOn(entry -> orderIds.contains(entry.getAggregateId()))
					.extracting(OrderOutBox::getStatus)
					.containsOnly(OrderOutBox.OutboxStatus.SENT);
			});


		CustomerOrder orderCplFalseLow = orderRepository.findById(new OrderId(UUID.fromString(lowOrderId))).orElseThrow();
		CustomerOrder orderCplFalseNormal = orderRepository.findById(new OrderId(UUID.fromString(normalOrderId2))).orElseThrow();
		CustomerOrder orderCplTrueHigh= orderRepository.findById(new OrderId(UUID.fromString(highOrderId))).orElseThrow();
		CustomerOrder orderCplTrueNormal = orderRepository.findById(new OrderId(UUID.fromString(normalOrderId))).orElseThrow();

		assertThat(orderCplFalseLow.getStatus())
			.as("orderCplFalseLow (orderId=%s)", lowOrderId)
			.isEqualTo(OrderStatus.PARTIALLY_ALLOCATED);
		assertThat(orderCplFalseNormal.getStatus())
			.as("orderCplFalseNormal (orderId=%s)", normalOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderCplTrueHigh.getStatus())
			.as("orderCplTrueHigh (orderId=%s)", highOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderCplTrueNormal.getStatus())
			.as("orderCplTrueNormalNORMAL (orderId=%s)", normalOrderId)
			.isEqualTo(OrderStatus.ALLOCATION_FAILED);



	}


}

package com.stock.management.domain;

import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.OrderOutboxRepository;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CancelOrderRequest;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.CreateOrderResponse;
import com.stock.management.order.dto.LineItemRequest;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.domain.Sku;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OrderReceivedBatchListenerIT {
	private static final String TEST_TOPIC = KafkaTopics.ORDER_RECEIVED_TEST02;

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	@Autowired private  OrderService orderService;
	@Autowired private  OrderRepository orderRepository;
	@Autowired private  SkuRepository skuRepository;
	@Autowired private  OrderOutboxRepository outboxRepository;
	@Autowired private  AllocationItemRepository allocationItemRepository;




	@BeforeAll
	static void purgeTestTopic(@Autowired KafkaAdmin kafkaAdmin) throws Exception {
		try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
			Map<String, Object> endOffsets = admin.listOffsets(
					Map.of(new TopicPartition(KafkaTopics.ORDER_RECEIVED_TEST02, 0), OffsetSpec.latest())
				).all().get(10, TimeUnit.SECONDS)
				.entrySet().stream()
				.collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().offset()));

			long latestOffset = (long) endOffsets.get(KafkaTopics.ORDER_RECEIVED_TEST02 + "-0");

			admin.deleteRecords(Map.of(
				new TopicPartition(KafkaTopics.ORDER_RECEIVED_TEST02, 0),
				RecordsToDelete.beforeOffset(latestOffset)
			)).all().get(10, TimeUnit.SECONDS);
		}
	}


	@BeforeEach
	void cleanUp() {
		outboxRepository.deleteAll();
		orderRepository.deleteAll();
		skuRepository.deleteAll();
		allocationItemRepository.deleteAll();
	}

	private LineItemRequest createLineItemRequest(String productNr, int quantity, BigDecimal unitPrice){
		return new LineItemRequest(productNr, quantity, unitPrice);
	}

	private List<Sku> createAndSaveSku(){
		return skuRepository.saveAll(List.of(
			Sku.create(new ProductNr("PROD01"), new Quantity(90), "WA-01"),
			Sku.create(new ProductNr("PROD02"), new Quantity(80), "WA-02"),
			Sku.create(new ProductNr("PROD03"), new Quantity(20), "WA-03")
		));
	}

	private List<CustomerOrder> createCustomerOrders() {
		LineItemRequest l1 = createLineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest l2 = createLineItemRequest("PROD02", 40, new BigDecimal("10.00"));
		LineItemRequest l3 = createLineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest l4 = createLineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest l5 = createLineItemRequest("PROD01", 30, new BigDecimal("10.00"));
		LineItemRequest l6 = createLineItemRequest("PROD02", 20, new BigDecimal("10.00"));

		LineItemRequest l7 = createLineItemRequest("PROD01", 20, new BigDecimal("10.00"));
		LineItemRequest l8 = createLineItemRequest("PROD02", 10, new BigDecimal("10.00"));

		CustomerOrder saveCustomerOrder = CustomerOrder.create(Priority.LOW, false, "EUR", List.of(l1, l2));
		CustomerOrder saveCustomerOrder02 = CustomerOrder.create(Priority.HIGH, true, "EUR", List.of(l3, l4));
		CustomerOrder saveCustomerOrder03 = CustomerOrder.create(Priority.NORMAL, true, "EUR", List.of(l5, l6));
		return List.of(saveCustomerOrder, saveCustomerOrder02,saveCustomerOrder03);
	}




	private record OrderLineRequest(
		String orderLineItemId,
		String sku,
		int quantity,
		BigDecimal unitPrice,
		LineItemStatus status
	) {}


	/**
	 * Variante END-TO-END : passe par le vrai chemin production, outbox inclus.
	 * OutboxPublisher.processOutboxEvents() tourne toutes les 3s (@Scheduled fixedDelay=3000)
	 * et traite jusqu'à 50 entrées PENDING/FAILED par cycle — largement suffisant pour
	 * absorber nos 3 commandes de test en UN SEUL cycle, pas besoin d'attendre plusieurs
	 * passages. Le timeout de 20s reste confortable (~6 cycles) sans être excessif.
	 * Utile pour valider le pipeline complet ; le test ci-dessus reste préférable
	 * pour un feedback rapide sur le listener seul, isolé de l'outbox.
	 */
	@Test
	@DisplayName("End-to-end : receiveOrder() → outbox → poller (3s) → Kafka → listener → allocation, sans contourner l'outbox")
	void fullPipeline_fromReceiveOrderThroughOutboxToAllocation_eventuallyAllocatesAllOrders() throws InterruptedException, ExecutionException, TimeoutException {
		// GIVEN : stock préexistant pour 3 produits distincts
		// create and save Sku
		// create and save Sku
		createAndSaveSku();

		// saved tree Customer order parelelle
		LineItemRequest item01 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item02 = new LineItemRequest("PROD02", 40, new BigDecimal("10.00"));
		LineItemRequest item03 = new LineItemRequest("PROD01", 50, new BigDecimal("10.00"));
		LineItemRequest item04 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item05 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item06 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item07 = createLineItemRequest("PROD01", 50, new BigDecimal("10.00"));
		LineItemRequest item08 = createLineItemRequest("PROD02", 50, new BigDecimal("10.00"));

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



		List<String> orderIds = List.of(lowOrderId, highOrderId, normalOrderId);

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


		CustomerOrder orderLow = orderRepository.findById(new OrderId(UUID.fromString(lowOrderId))).orElseThrow();
		CustomerOrder orderLow2 = orderRepository.findById(new OrderId(UUID.fromString(normalOrderId2))).orElseThrow();
		CustomerOrder orderHigh = orderRepository.findById(new OrderId(UUID.fromString(highOrderId))).orElseThrow();
		CustomerOrder orderNormal = orderRepository.findById(new OrderId(UUID.fromString(normalOrderId))).orElseThrow();

		assertThat(orderLow.getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderLow2.getStatus())
			.as("NORMAL (orderId=%s)", normalOrderId)
			.isEqualTo(OrderStatus.CANCELLED);
		assertThat(orderHigh.getStatus())
			.as("HIGH (orderId=%s)", highOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderNormal.getStatus())
			.as("NORMAL (orderId=%s)", normalOrderId)
			.isEqualTo(OrderStatus.ALLOCATION_FAILED);


		// THEN : l'outbox doit être passée précisément à SENT
		assertThat(outboxRepository.findAll())
			.as("Une fois publiées avec succès par le poller, les entrées outbox doivent être SENT")
			.filteredOn(entry -> orderIds.contains(entry.getAggregateId()))
			.extracting(OrderOutBox::getStatus)
			.containsOnly(OrderOutBox.OutboxStatus.SENT);
	}


	@Test
	void deleteTest(){
		// GIVEN : stock préexistant pour 3 produits distincts
		// create and save Sku
		// create and save Sku
		createAndSaveSku();

		// saved tree Customer order parelelle
		LineItemRequest item01 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item02 = new LineItemRequest("PROD02", 40, new BigDecimal("10.00"));
		LineItemRequest item03 = new LineItemRequest("PROD01", 50, new BigDecimal("10.00"));
		LineItemRequest item04 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item05 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item06 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item07 = createLineItemRequest("PROD01", 50, new BigDecimal("10.00"));
		LineItemRequest item08 = createLineItemRequest("PROD02", 50, new BigDecimal("10.00"));

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

		orderService.cancelOrder(  new OrderId(UUID.fromString(normalOrderId2


			)),
			new CancelOrderRequest("client01", "client request", CancellationSource.CUSTOMER_APP, null));

		CustomerOrder orderLow = orderRepository.findByIdWithLineItems(new OrderId(UUID.fromString(lowOrderId))).orElseThrow();
		CustomerOrder orderHigh = orderRepository.findByIdWithLineItems(new OrderId(UUID.fromString(highOrderId))).orElseThrow();
		CustomerOrder orderNormal = orderRepository.findByIdWithLineItems(new OrderId(UUID.fromString(normalOrderId))).orElseThrow();
		CustomerOrder orderNormal2 = orderRepository.findByIdWithLineItems(new OrderId(UUID.fromString(normalOrderId2))).orElseThrow();


		assertThat(orderLow.getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderHigh.getStatus())
			.as("HIGH (orderId=%s)", highOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderNormal.getStatus())
			.as("NORMAL (orderId=%s)", normalOrderId)
			.isEqualTo(OrderStatus.ALLOCATION_FAILED);
		assertThat(orderNormal2.getStatus())
			.as("NORMAL (orderId=%s)", normalOrderId)
			.isEqualTo(OrderStatus.CANCELLED);

		assertThat(orderLow.getLineItems().getFirst().getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.FULLY_ALLOCATED);

		assertThat(orderLow.getLineItems().get(1).getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.FULLY_ALLOCATED);

		assertThat(orderHigh.getLineItems().getFirst().getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.FULLY_ALLOCATED);
		assertThat(orderHigh.getLineItems().get(1).getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.FULLY_ALLOCATED);


		assertThat(orderNormal2.getLineItems().getFirst().getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.CANCELLED);

		assertThat(orderNormal2.getLineItems().get(1).getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.CANCELLED);

		assertThat(orderNormal.getLineItems().getFirst().getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.NOT_ALLOCATED);

		assertThat(orderNormal.getLineItems().get(1).getStatus())
			.as("LOW (orderId=%s)", lowOrderId)
			.isEqualTo(LineItemStatus.NOT_ALLOCATED);

	}

}

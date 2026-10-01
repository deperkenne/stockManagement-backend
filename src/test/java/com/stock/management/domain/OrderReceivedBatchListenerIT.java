package com.stock.management.domain;

import com.stock.management.allocationLine.AllocationItem;
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
import com.stock.management.sku.SkuService;
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
import org.springframework.test.context.transaction.TestTransaction;
import org.springframework.transaction.annotation.Transactional;

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
	private static final String TEST_TOPIC = KafkaTopics.ORDER_RECEIVED_TEST03;

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	@Autowired private  OrderService orderService;
	@Autowired private  OrderRepository orderRepository;
	@Autowired private  SkuRepository skuRepository;
	@Autowired private  OrderOutboxRepository outboxRepository;
	@Autowired private  AllocationItemRepository allocationItemRepository;
	@Autowired private SkuService skuService;




	@BeforeAll
	static void purgeTestTopic(@Autowired KafkaAdmin kafkaAdmin) throws Exception {
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

	private void createAndSaveSku() {
		 skuService.saveSkus(List.of(
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
	) {}


	@RepeatedTest(value = 8, name = "Run {currentRepetition}/{totalRepetitions}")
	void deleteTest(){
		// GIVEN : stock préexistant pour 3 produits distincts
		// create and save Sku
		// create and save Sku
		createAndSaveSku();


		// saved tree Customer order parelelle
		// saved tree Customer order parelelle
		LineItemRequest item01 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item02 = new LineItemRequest("PROD02", 60, new BigDecimal("10.00"));
		LineItemRequest item03 = new LineItemRequest("PROD01", 50, new BigDecimal("10.00"));
		LineItemRequest item04 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item05 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item06 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item07 = createLineItemRequest("PROD01", 60, new BigDecimal("10.00"));
		LineItemRequest item08 = createLineItemRequest("PROD02", 40, new BigDecimal("10.00"));

		CreateOrderRequest createOrderRequestLowF = new CreateOrderRequest(
			Priority.LOW, false, "EUR", List.of(item01, item02));

		CreateOrderRequest createOrderRequestHighT = new CreateOrderRequest(
			Priority.HIGH, true, "EUR", List.of(item03, item04));

		CreateOrderRequest createOrderRequestNorT = new CreateOrderRequest(
			Priority.NORMAL, true, "EUR", List.of(item05, item06));

		CreateOrderRequest createOrderRequestNorF = new CreateOrderRequest(
			Priority.NORMAL, false, "EUR", List.of( item07, item08));


		String lowFalseOrderId = orderService.receiveOrder(createOrderRequestLowF).orderId();
		String highTrueOrderId = orderService.receiveOrder(createOrderRequestHighT).orderId();
		String normalTrueOrderId = orderService.receiveOrder(createOrderRequestNorT).orderId();
		String normalFalseOrderId = orderService.receiveOrder(createOrderRequestNorF).orderId();

		List<String> orderIds = List.of(lowFalseOrderId, highTrueOrderId, normalTrueOrderId,normalFalseOrderId);

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

		orderService.cancelOrder(
			new OrderId(UUID.fromString(normalFalseOrderId)),
			new CancelOrderRequest
				(
					"client01",
					"client request",
					CancellationSource.CUSTOMER_APP,
					null
				)
		);


		CustomerOrder orderCplFalseLow = orderRepository.findById(new OrderId(UUID.fromString(lowFalseOrderId))).orElseThrow();
		CustomerOrder orderCplTrueHigh  = orderRepository.findById(new OrderId(UUID.fromString(highTrueOrderId))).orElseThrow();
		CustomerOrder orderCplTrueNormal = orderRepository.findById(new OrderId(UUID.fromString(normalTrueOrderId))).orElseThrow();
		CustomerOrder orderCplFalseNormal  = orderRepository.findById(new OrderId(UUID.fromString(normalFalseOrderId))).orElseThrow();

		assertThat(orderCplFalseLow.getStatus())
			.as("orderCplFalseLow (orderId=%s)", lowFalseOrderId)
			.isEqualTo(OrderStatus.PARTIALLY_ALLOCATED);
		assertThat(orderCplTrueHigh.getStatus())
			.as("orderCplTrueHigh(orderId=%s)", highTrueOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderCplTrueNormal.getStatus())
			.as("orderCplTrueNormal (orderId=%s)", normalTrueOrderId)
			.isEqualTo(OrderStatus.FULLY_ALLOCATED);
		assertThat(orderCplFalseNormal.getStatus())
			.as("orderCplTrueNormal (orderId=%s)", normalFalseOrderId)
			.isEqualTo(OrderStatus.CANCELLED);

		assertAllocationItemB(
			                   List.of
			                       (

				                    UUID.fromString(highTrueOrderId),
				                    UUID.fromString(normalTrueOrderId),
			                        UUID.fromString(normalFalseOrderId),
									   UUID.fromString(lowFalseOrderId)

			                  )
		);
	}


	private void assertAllocationItemB(List<UUID> orderIds){
		List<AllocationItem> allocationItemsB = allocationItemRepository.findAlreadyAllocatedOrders(orderIds);

		Map<UUID, List<AllocationItem>> allocationItemsByOrderId = allocationItemsB.stream()
			.collect(Collectors.groupingBy(
				item -> item.getOrderId(),      // extraction de la clé UUID
				Collectors.toCollection(ArrayList::new)  // force ArrayList comme type de collection pour chaque groupe
			));



		List<AllocationItem> allocationItems1 = allocationItemsByOrderId.get(orderIds.getFirst());
		List<AllocationItem> allocationItems2 = allocationItemsByOrderId.get(orderIds.get(1));
		List<AllocationItem> allocationItems3 = allocationItemsByOrderId.get(orderIds.get(2));
		List<AllocationItem> allocationItems4 = allocationItemsByOrderId.get(orderIds.get(3));


		assertThat(allocationItemsB.size())
			.as("allocationsize (orderId=%s)", "Before")
			.isEqualTo(7);

		assertThat(allocationItems1.getFirst().getQuantity())
			.as("getQuantityHighT (orderId=%s)", "")
			.isEqualTo(50);

		assertThat(allocationItems1.get(1).getQuantity())
			.as("getQuantityHighT (orderId=%s)", "")
			.isEqualTo(20);



		assertThat(allocationItems2.getFirst().getQuantity())
			.as("getQuantityHighT (orderId=%s)", "")
			.isEqualTo(40);


		assertThat(allocationItems2.get(1).getQuantity())
			.as("getQuantityHighT (orderId=%s)", "")
			.isEqualTo(20);

		assertThat(allocationItems3.getFirst().getQuantity())
			.as("getQuantityNorF (orderId=%s)", "")
			.isEqualTo(40);

	}

	private void AssertAllocationItemA(List<OrderId> orderIds){

	}

}

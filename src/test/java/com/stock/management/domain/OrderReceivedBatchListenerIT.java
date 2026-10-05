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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.*;
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



	private record SkuStock(String productNr, int quantity, String warehouse) {}

	private record OrderLine(String productNr, int quantity) {}

	private record OrderDef(String label, Priority priority, boolean urgent, List<OrderLine> lines) {}

	private record AllocationExpectation(String orderLabel, List<Integer> expectedQuantities) {}

	private record AllocationScenario(
		String name,
		List<SkuStock> stock,
		List<OrderDef> orders,
		Map<String, OrderStatus> expectedStatusByLabel,
		List<AllocationExpectation> expectedAllocations,

		String cancelledOrderLabel // nullable si aucune annulation
	) {
		@Override public String toString() { return name; }
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("allocationScenarios")
	void allocationRespectsPriority(AllocationScenario scenario) {

		// GIVEN : stock propre à ce scénario
		skuService.saveSkus(scenario.stock().stream()
			.map(s -> Sku.create(new ProductNr(s.productNr()), new Quantity(s.quantity()), s.warehouse()))
			.toList());

		// WHEN : soumission des commandes dans l'ordre défini par le scénario
		Map<String, String> orderIdByLabel = new LinkedHashMap<>();
		for (OrderDef def : scenario.orders()) {
			List<LineItemRequest> items = def.lines().stream()
				.map(l -> new LineItemRequest(l.productNr(), l.quantity(), new BigDecimal("10.00")))
				.toList();
			String orderId = orderService
				.receiveOrder(new CreateOrderRequest(def.priority(), def.urgent(), "EUR", items))
				.orderId();
			orderIdByLabel.put(def.label(), orderId);
		}

		List<String> orderIds = new ArrayList<>(orderIdByLabel.values());

		// THEN : toutes les commandes sortent de PENDING
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

		await()
			.atMost(15, TimeUnit.SECONDS)
			.pollInterval(200, TimeUnit.MILLISECONDS)
			.untilAsserted(() ->
				assertThat(outboxRepository.findAll())
					.filteredOn(entry -> orderIds.contains(entry.getAggregateId()))
					.extracting(OrderOutBox::getStatus)
					.containsOnly(OrderOutBox.OutboxStatus.SENT)
			);

		if (scenario.cancelledOrderLabel() != null) {
			orderService.cancelOrder(
				new OrderId(UUID.fromString(orderIdByLabel.get(scenario.cancelledOrderLabel()))),
				new CancelOrderRequest("client01", "client request", CancellationSource.CUSTOMER_APP, null)
			);
		}

		scenario.expectedStatusByLabel().forEach((label, expectedStatus) -> {
			CustomerOrder order = orderRepository
				.findById(new OrderId(UUID.fromString(orderIdByLabel.get(label))))
				.orElseThrow();
			assertThat(order.getStatus())
				.as("statut de %s (orderId=%s)", label, orderIdByLabel.get(label))
				.isEqualTo(expectedStatus);
		});

		if (!scenario.expectedAllocations().isEmpty()) {
			List<UUID> uuids = scenario.expectedAllocations().stream()
				.map(exp -> UUID.fromString(orderIdByLabel.get(exp.orderLabel())))
				.toList();
			Map<UUID, List<AllocationItem>> byOrder = allocationItemRepository
				.findAlreadyAllocatedOrders(uuids).stream()
				.collect(Collectors.groupingBy(AllocationItem::getOrderId, Collectors.toCollection(ArrayList::new)));

			for (AllocationExpectation exp : scenario.expectedAllocations()) {
				UUID id = UUID.fromString(orderIdByLabel.get(exp.orderLabel()));
				List<Integer> actual = byOrder.getOrDefault(id, List.of()).stream()
					.map(AllocationItem::getQuantity)
					.toList();
				assertThat(actual)
					.as("quantités allouées pour %s", exp.orderLabel())
					.isEqualTo(exp.expectedQuantities());
			}
		}
	}

	static Stream<AllocationScenario> allocationScenarios() {
		return Stream.of(baselineScenario(),priorityShuffleScenario(),priorityScenario());
		// Ajoute tes propres scénarios ici une fois que tu as vérifié
		// manuellement (ou via un run exploratoire) le résultat réel —
		// je ne les invente pas pour éviter de te faire valider un faux calcul.
	}

	private static AllocationScenario baselineScenario() {
		return new AllocationScenario(
			"stock 90/80/20 - 4 commandes mixtes (scénario original)",
			List.of(
				new SkuStock("PROD01", 90, "WA-01"),
				new SkuStock("PROD02", 80, "WA-02"),
				new SkuStock("PROD03", 20, "WA-03")
			),
			List.of(
				new OrderDef("lowFalse", Priority.LOW, false,
					List.of(new OrderLine("PROD01", 40), new OrderLine("PROD02", 60))),
				new OrderDef("highTrue", Priority.HIGH, true,
					List.of(new OrderLine("PROD01", 50), new OrderLine("PROD02", 20))),
				new OrderDef("normalTrue", Priority.NORMAL, true,
					List.of(new OrderLine("PROD01", 40), new OrderLine("PROD02", 20))),
				new OrderDef("normalFalse", Priority.NORMAL, false,
					List.of(new OrderLine("PROD01", 60), new OrderLine("PROD02", 40)))
			),
			Map.of(
				"lowFalse", OrderStatus.PARTIALLY_ALLOCATED,
				"highTrue", OrderStatus.FULLY_ALLOCATED,
				"normalTrue", OrderStatus.FULLY_ALLOCATED,
				"normalFalse", OrderStatus.CANCELLED
			),
			List.of(
				new AllocationExpectation("highTrue", List.of(50, 20)),
				new AllocationExpectation("normalTrue", List.of(40, 20)),
				new AllocationExpectation("normalFalse", List.of(40))
			),
			"normalFalse"
		);
	}

	private static AllocationScenario priorityShuffleScenario() {
		return new AllocationScenario(
			"stock 90/80/20 - mêmes lignes, priorités changées (order1=HIGH,order2=NORMAL,order3=LOW,order4=HIGH)",
			List.of(
				new SkuStock("PROD01", 90, "WA-01"),
				new SkuStock("PROD02", 80, "WA-02"),
				new SkuStock("PROD03", 20, "WA-03")
			),
			List.of(
				new OrderDef("order1", Priority.HIGH, false,
					List.of(new OrderLine("PROD01", 40), new OrderLine("PROD02", 60))),
				new OrderDef("order2", Priority.NORMAL, true,
					List.of(new OrderLine("PROD01", 50), new OrderLine("PROD02", 20))),
				new OrderDef("order3", Priority.LOW, true,
					List.of(new OrderLine("PROD01", 40), new OrderLine("PROD02", 20))),
				new OrderDef("order4", Priority.HIGH, false,
					List.of(new OrderLine("PROD01", 60), new OrderLine("PROD02", 40)))
			),
			Map.of(
				"order1", OrderStatus.FULLY_ALLOCATED,
				"order2", OrderStatus.FULLY_ALLOCATED,
				"order3", OrderStatus.ALLOCATION_FAILED,
				"order4", OrderStatus.CANCELLED
			),
			List.of(
				new AllocationExpectation("order1", List.of(40, 60)),
				new AllocationExpectation("order2", List.of(50, 20)),
				new AllocationExpectation("order4", List.of(50, 0, 20, 0))
			),
			"order4"
		);
	}


	private static AllocationScenario priorityScenario() {
		return new AllocationScenario(
			"stock 90/80/20 - mêmes lignes, priorités changées (order1=HIGH,order2=NORMAL,order3=LOW,order4=HIGH)",
			List.of(
				new SkuStock("PROD01", 90, "WA-01"),
				new SkuStock("PROD02", 80, "WA-02"),
				new SkuStock("PROD01", 70, "WA-03")
			),
			List.of(
				new OrderDef("order1", Priority.HIGH, false,
					List.of(new OrderLine("PROD01", 40), new OrderLine("PROD02", 60))),
				new OrderDef("order2", Priority.NORMAL, true,
					List.of(new OrderLine("PROD01", 50), new OrderLine("PROD02", 20))),
				new OrderDef("order3", Priority.LOW, true,
					List.of(new OrderLine("PROD01", 40), new OrderLine("PROD02", 20))),
				new OrderDef("order5", Priority.HIGH, false,
					List.of(new OrderLine("PROD01", 80), new OrderLine("PROD02", 50))),
				new OrderDef("order4", Priority.HIGH, false,
					List.of(new OrderLine("PROD01", 60), new OrderLine("PROD02", 40)))
			),
			Map.of(
				"order1", OrderStatus.FULLY_ALLOCATED,
				"order2", OrderStatus.FULLY_ALLOCATED,
				"order3", OrderStatus.ALLOCATION_FAILED,
				"order4", OrderStatus.CANCELLED,
				"order5", OrderStatus.PARTIALLY_ALLOCATED
			),
			List.of(
				new AllocationExpectation("order1", List.of(40, 60)),
				new AllocationExpectation("order2", List.of(50, 20)),
				new AllocationExpectation("order4", List.of(50, 30, 20, 0)),
		        new AllocationExpectation("order5", List.of(40, 0, 20))
			),
			"order4"
		);
	}

}

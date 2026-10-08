package com.stock.management.domain;


import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.allocationLine.AllocationItemStatus;
import com.stock.management.kafka.config.KafkaTopics;
import com.stock.management.order.LineItemNotFoundException;
import com.stock.management.order.OrderCancellationException;
import com.stock.management.order.OrderOutboxRepository;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CancelOrderRequest;
import com.stock.management.order.dto.CancelOrderResponse;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.LineItemRequest;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.SkuService;
import com.stock.management.sku.domain.Sku;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CancelLineItemsIT {

	private static final CancellationSource CANCELLATION_SOURCE = CancellationSource.CUSTOMER_APP;

	// Stock + commande communs : PROD01 entièrement alloué (40/40), PROD02 partiellement (10/30)
	// -> après allocation : commande PARTIALLY_ALLOCATED, stock restant PROD01 = 10, PROD02 = 0
	private static final List<SkuStock> PARTIAL_STOCK = List.of(
		new SkuStock("PROD01", 50, "WA-01"),
		new SkuStock("PROD02", 10, "WA-02")
	);
	private static final List<OrderLine> ORDER_LINES = List.of(
		new OrderLine("PROD01", 40),
		new OrderLine("PROD02", 30)
	);

	@Autowired private OrderService orderService;
	@Autowired private OrderRepository orderRepository;
	@Autowired private SkuRepository skuRepository;
	@Autowired private SkuService skuService;
	@Autowired private OrderOutboxRepository outboxRepository;
	@Autowired private AllocationItemRepository allocationItemRepository;


	@BeforeAll
	static void purgeTestTopic(@Autowired KafkaAdmin kafkaAdmin) throws Exception {
		try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
			TopicPartition partition = new TopicPartition(KafkaTopics.ORDER_RECEIVED_TEST03, 0);
			long latestOffset = admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
				.all().get(10, TimeUnit.SECONDS)
				.get(partition).offset();

			admin.deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(latestOffset)))
				.all().get(10, TimeUnit.SECONDS);
		}
	}

	@BeforeEach
	void cleanUp() {
		outboxRepository.deleteAll();
		orderRepository.deleteAll();
		skuRepository.deleteAll();
		allocationItemRepository.deleteAll();
	}


	private record SkuStock(String productNr, int quantity, String warehouse) {}

	private record OrderLine(String productNr, int quantity) {}

	private record CancelScenario(
		String name,
		List<SkuStock> stock,
		List<OrderLine> lines,
		OrderStatus statusBeforeCancel,
		List<String> productsToCancel,
		boolean cancelTwice,                               // true : rejoue la même annulation (idempotence)
		OrderStatus expectedOrderStatus,
		Map<String, LineItemStatus> expectedLineStatusByProduct,
		Map<String, Integer> expectedStockByProduct,
		int expectedReleasedAllocations                    // valeur renvoyée par le DERNIER appel
	) {
		@Override public String toString() { return name; }
	}

	private record ReplayScenario(
		String name,
		List<SkuStock> stock,
		List<OrderLine> cancelledOrderLines,               // commande A : on annule une ou plusieurs de ses lignes
		OrderStatus cancelledOrderStatusBefore,
		List<String> productsToCancel,
		List<OrderLine> waitingOrderLines,                 // commande B : attend le stock libéré par A
		boolean waitingOrderCompleteDelivery,
		OrderStatus waitingOrderStatusBefore,
		Map<String, Integer> expectedStockByProduct,
		OrderStatus expectedWaitingOrderStatus,
		Map<String, LineItemStatus> expectedWaitingLineStatusByProduct,
		Map<String, Integer> expectedWaitingAllocatedQtyByProduct
	) {
		@Override public String toString() { return name; }
	}

	private record RejectionScenario(
		String name,
		List<SkuStock> stock,
		List<OrderLine> lines,
		OrderStatus statusBeforeCancel,
		List<String> productsToCancel,
		boolean withUnknownLine,                           // true : ajoute un lineItemId étranger à la commande
		Class<? extends RuntimeException> expectedException
	) {
		@Override public String toString() { return name; }
	}


	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("cancelScenarios")
	void cancelLineItemsReleasesStockAndUpdatesStatuses(CancelScenario scenario) {

		// GIVEN : une commande allouée dans l'état attendu par le scénario
		givenStock(scenario.stock());
		UUID orderId = givenAllocatedOrder(scenario.lines(), false, scenario.statusBeforeCancel());
		List<UUID> lineIdsToCancel = lineIdsOf(orderId, scenario.productsToCancel());

		// WHEN : annulation des lignes (rejouée une 2e fois pour le scénario d'idempotence)
		CancelOrderResponse response = orderService.cancelLineItems(orderId, cancelRequest(lineIdsToCancel));
		if (scenario.cancelTwice()) {
			response = orderService.cancelLineItems(orderId, cancelRequest(lineIdsToCancel));
		}

		// THEN : statuts, stock et allocations reflètent exactement l'annulation
		CustomerOrder order = loadOrderWithLines(orderId);

		assertThat(order.getStatus())
			.as("statut de la commande")
			.isEqualTo(scenario.expectedOrderStatus());
		assertThat(lineStatusByProduct(order))
			.as("statut de chaque ligne")
			.isEqualTo(scenario.expectedLineStatusByProduct());
		assertThat(stockByProduct())
			.as("stock disponible par produit (après libération ET après le retry)")
			.isEqualTo(scenario.expectedStockByProduct());
		assertThat(response.releasedAllocations())
			.as("nombre d'allocations libérées")
			.isEqualTo(scenario.expectedReleasedAllocations());
		assertThat(allocationItemRepository.findAllByLineItemIdIn(lineIdsToCancel))
			.as("toutes les allocations des lignes annulées, y compris WAITING_STOCK")
			.extracting(AllocationItem::getStatus)
			.containsOnly(AllocationItemStatus.CANCELLED);

		if (scenario.expectedOrderStatus() == OrderStatus.CANCELLED) {
			assertThat(order.getCancelledAt()).as("cancelledAt").isNotNull();
			assertThat(order.getCancellationSource()).as("cancellationSource").isEqualTo(CANCELLATION_SOURCE);
		}
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("replayScenarios")
	void cancelLineItemsReleasesStockAndReplaysWaitingOrders(ReplayScenario scenario) {

		// GIVEN : A est allouée en premier, puis B (créées l'une après l'autre -> allocation déterministe)
		givenStock(scenario.stock());
		UUID cancelledOrderId = givenAllocatedOrder(
			scenario.cancelledOrderLines(), false, scenario.cancelledOrderStatusBefore());
		UUID waitingOrderId = givenAllocatedOrder(
			scenario.waitingOrderLines(), scenario.waitingOrderCompleteDelivery(), scenario.waitingOrderStatusBefore());

		// WHEN : annulation de lignes de A -> stock rendu (transaction A) puis retry après commit (transaction B)
		orderService.cancelLineItems(cancelledOrderId, cancelRequest(lineIdsOf(cancelledOrderId, scenario.productsToCancel())));

		// THEN : le stock rendu a été réalloué à B, et B est à jour
		CustomerOrder waitingOrder = loadOrderWithLines(waitingOrderId);

		assertThat(stockByProduct())
			.as("stock disponible par produit après libération puis replay")
			.isEqualTo(scenario.expectedStockByProduct());
		assertThat(waitingOrder.getStatus())
			.as("statut de la commande rejouée")
			.isEqualTo(scenario.expectedWaitingOrderStatus());
		assertThat(lineStatusByProduct(waitingOrder))
			.as("statut des lignes de la commande rejouée")
			.isEqualTo(scenario.expectedWaitingLineStatusByProduct());
		assertThat(allocatedQtyOnSkuByProduct(waitingOrderId))
			.as("quantité réservée sur un emplacement (skuId) par produit pour la commande rejouée")
			.isEqualTo(scenario.expectedWaitingAllocatedQtyByProduct());
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("rejectionScenarios")
	void cancelLineItemsRejectsInvalidRequestWithoutSideEffect(RejectionScenario scenario) {

		// GIVEN : une commande allouée + photo de l'état avant la tentative d'annulation
		givenStock(scenario.stock());
		UUID orderId = givenAllocatedOrder(scenario.lines(), false, scenario.statusBeforeCancel());
		List<UUID> lineIdsToCancel = new ArrayList<>(lineIdsOf(orderId, scenario.productsToCancel()));
		if (scenario.withUnknownLine()) {
			lineIdsToCancel.add(UUID.randomUUID());
		}
		Map<String, LineItemStatus> lineStatusBefore = lineStatusByProduct(loadOrderWithLines(orderId));
		Map<String, Integer> stockBefore = stockByProduct();

		// WHEN / THEN : la demande est refusée
		assertThatThrownBy(() -> orderService.cancelLineItems(orderId, cancelRequest(lineIdsToCancel)))
			.isInstanceOf(scenario.expectedException());

		// THEN : rien n'a bougé (pas d'annulation partielle)
		CustomerOrder order = loadOrderWithLines(orderId);

		assertThat(order.getStatus())
			.as("statut de la commande inchangé")
			.isEqualTo(scenario.statusBeforeCancel());
		assertThat(lineStatusByProduct(order))
			.as("statut des lignes inchangé")
			.isEqualTo(lineStatusBefore);
		assertThat(stockByProduct())
			.as("stock inchangé")
			.isEqualTo(stockBefore);
	}


	static Stream<CancelScenario> cancelScenarios() {
		return Stream.of(
			new CancelScenario(
				"PARTIALLY_ALLOCATED - annuler la ligne entièrement allouée rend ses 40 PROD01",
				PARTIAL_STOCK, ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED,
				List.of("PROD01"), false,
				OrderStatus.PARTIALLY_ALLOCATED,
				Map.of("PROD01", LineItemStatus.CANCELLED, "PROD02", LineItemStatus.PARTIALLY_ALLOCATED),
				Map.of("PROD01", 50, "PROD02", 0),
				1
			),
			new CancelScenario(
				"PARTIALLY_ALLOCATED - annuler la ligne partielle : commande FULLY_ALLOCATED, stock rendu non repris par le retry",
				PARTIAL_STOCK, ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED,
				List.of("PROD02"), false,
				OrderStatus.FULLY_ALLOCATED,
				Map.of("PROD01", LineItemStatus.FULLY_ALLOCATED, "PROD02", LineItemStatus.CANCELLED),
				Map.of("PROD01", 10, "PROD02", 10),
				1
			),
			new CancelScenario(
				"PARTIALLY_ALLOCATED - annuler toutes les lignes : commande CANCELLED et tout le stock rendu",
				PARTIAL_STOCK, ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED,
				List.of("PROD01", "PROD02"), false,
				OrderStatus.CANCELLED,
				Map.of("PROD01", LineItemStatus.CANCELLED, "PROD02", LineItemStatus.CANCELLED),
				Map.of("PROD01", 50, "PROD02", 10),
				2
			),
			new CancelScenario(
				"idempotence - rejouer l'annulation ne libère pas le stock une 2e fois",
				PARTIAL_STOCK, ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED,
				List.of("PROD01"), true,
				OrderStatus.PARTIALLY_ALLOCATED,
				Map.of("PROD01", LineItemStatus.CANCELLED, "PROD02", LineItemStatus.PARTIALLY_ALLOCATED),
				Map.of("PROD01", 50, "PROD02", 0),
				0
			)
		);
	}

	static Stream<ReplayScenario> replayScenarios() {
		return Stream.of(
			new ReplayScenario(
				"B partielle (WAITING_STOCK 20 PROD01) : l'annulation de A.PROD01 rend 40, B reçoit ses 20 manquants",
				PARTIAL_STOCK,
				ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED, List.of("PROD01"),
				// B : 10 PROD01 restants -> 10 alloués, 20 en attente
				List.of(new OrderLine("PROD01", 30)), false, OrderStatus.PARTIALLY_ALLOCATED,
				// PROD01 : 0 + 40 rendus - 20 réalloués à B = 20
				Map.of("PROD01", 20, "PROD02", 0),
				OrderStatus.FULLY_ALLOCATED,
				Map.of("PROD01", LineItemStatus.FULLY_ALLOCATED),
				Map.of("PROD01", 30)
			),
			new ReplayScenario(
				"B ALLOCATION_FAILED (livraison complète, 30 PROD01) : l'annulation de A.PROD01 permet de rejouer B entièrement",
				PARTIAL_STOCK,
				ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED, List.of("PROD01"),
				// B : 10 PROD01 restants < 30 -> tout ou rien refusé, aucun stock pris
				List.of(new OrderLine("PROD01", 30)), true, OrderStatus.ALLOCATION_FAILED,
				// PROD01 : 10 + 40 rendus - 30 alloués à B = 20
				Map.of("PROD01", 20, "PROD02", 0),
				OrderStatus.FULLY_ALLOCATED,
				Map.of("PROD01", LineItemStatus.FULLY_ALLOCATED),
				Map.of("PROD01", 30)
			),
			new ReplayScenario(
				"B avec ligne NOT_ALLOCATED (0 PROD01 dispo) : l'annulation de A.PROD01 rejoue la ligne jamais allouée",
				List.of(
					new SkuStock("PROD01", 40, "WA-01"),
					new SkuStock("PROD02", 10, "WA-02"),
					new SkuStock("PROD03", 10, "WA-03")
				),
				ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED, List.of("PROD01"),
				// B : PROD01 épuisé par A -> ligne NOT_ALLOCATED ; PROD03 entièrement alloué
				List.of(new OrderLine("PROD01", 30), new OrderLine("PROD03", 5)), false, OrderStatus.PARTIALLY_ALLOCATED,
				// PROD01 : 0 + 40 rendus - 30 réalloués à B = 10 ; PROD03 : 10 - 5 = 5
				Map.of("PROD01", 10, "PROD02", 0, "PROD03", 5),
				OrderStatus.FULLY_ALLOCATED,
				Map.of("PROD01", LineItemStatus.FULLY_ALLOCATED, "PROD03", LineItemStatus.FULLY_ALLOCATED),
				Map.of("PROD01", 30, "PROD03", 5)
			)
		);
	}

	static Stream<RejectionScenario> rejectionScenarios() {
		return Stream.of(
			new RejectionScenario(
				"ligne étrangère à la commande : LineItemNotFoundException, la ligne valide n'est pas annulée",
				PARTIAL_STOCK, ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED,
				List.of("PROD01"), true,
				LineItemNotFoundException.class
			),
			new RejectionScenario(
				"commande FULLY_ALLOCATED : OrderCancellationException",
				List.of(new SkuStock("PROD01", 50, "WA-01"), new SkuStock("PROD02", 40, "WA-02")),
				ORDER_LINES, OrderStatus.FULLY_ALLOCATED,
				List.of("PROD01"), false,
				OrderCancellationException.class
			),
			new RejectionScenario(
				"aucune ligne demandée : IllegalArgumentException",
				PARTIAL_STOCK, ORDER_LINES, OrderStatus.PARTIALLY_ALLOCATED,
				List.of(), false,
				IllegalArgumentException.class
			)
		);
	}


	// ─── Helpers ──────────────────────────────────────────────────────────────────

	private void givenStock(List<SkuStock> stock) {
		skuService.saveSkus(stock.stream()
			.map(s -> Sku.create(new ProductNr(s.productNr()), new Quantity(s.quantity()), s.warehouse()))
			.toList());
	}

	/** Crée la commande puis attend que l'allocation asynchrone (outbox -> Kafka) l'ait amenée au statut attendu. */
	private UUID givenAllocatedOrder(List<OrderLine> lines, boolean completeDelivery, OrderStatus expectedStatus) {
		List<LineItemRequest> items = lines.stream()
			.map(l -> new LineItemRequest(l.productNr(), l.quantity(), new BigDecimal("10.00")))
			.toList();
		UUID orderId = UUID.fromString(orderService
			.receiveOrder(new CreateOrderRequest(Priority.NORMAL, completeDelivery, "EUR", items))
			.orderId());

		await()
			.atMost(15, TimeUnit.SECONDS)
			.pollInterval(200, TimeUnit.MILLISECONDS)
			.untilAsserted(() -> assertThat(orderRepository.findById(new OrderId(orderId)).orElseThrow().getStatus())
				.as("statut après allocation (orderId=%s)", orderId)
				.isEqualTo(expectedStatus));

		return orderId;
	}

	private CustomerOrder loadOrderWithLines(UUID orderId) {
		return orderRepository.findByIdWithLineItems(new OrderId(orderId)).orElseThrow();
	}

	/** Retrouve les lignes par productNr puis par ID (jamais par position dans la liste JPA). */
	private List<UUID> lineIdsOf(UUID orderId, List<String> productNrs) {
		Map<String, UUID> lineIdByProduct = loadOrderWithLines(orderId).getLineItems().stream()
			.collect(Collectors.toMap(li -> li.getProductNr().getValue(), li -> li.getId().getValue()));
		return productNrs.stream().map(lineIdByProduct::get).toList();
	}

	private Map<String, LineItemStatus> lineStatusByProduct(CustomerOrder order) {
		return order.getLineItems().stream()
			.collect(Collectors.toMap(li -> li.getProductNr().getValue(), LineItem::getStatus));
	}

	private Map<String, Integer> stockByProduct() {
		return skuRepository.findAll().stream()
			.collect(Collectors.groupingBy(
				sku -> sku.getProductNr().getValue(),
				Collectors.summingInt(sku -> sku.getAvailableQuantity().getValue())));
	}

	/** Somme, par produit, des quantités réellement réservées sur un emplacement (allocations ALLOCATED avec skuId). */
	private Map<String, Integer> allocatedQtyOnSkuByProduct(UUID orderId) {
		return allocationItemRepository.findAlreadyAllocatedOrderId(orderId).stream()
			.filter(item -> item.getStatus() == AllocationItemStatus.ALLOCATED && item.getSkuId() != null)
			.collect(Collectors.groupingBy(
				item -> item.getProductNr().getValue(),
				Collectors.summingInt(AllocationItem::getQuantity)));
	}

	private CancelOrderRequest cancelRequest(List<UUID> lineItemIds) {
		return new CancelOrderRequest("client request", "client01", CANCELLATION_SOURCE, lineItemIds);
	}
}

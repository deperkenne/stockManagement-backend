package com.stock.management.domain;

/*
import com.stock.management.allocation.AllocationService;
import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
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

import org.springframework.core.env.Environment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.Fail.fail;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
public class CustomerOrderOptimisticLockingIT {
	    private static final int THREAD_COUNT = 20;

		@Autowired
		private OrderRepository orderRepository;
		@Autowired
		private OrderService orderService;

		@Autowired
		private OrderOutboxRepository orderOutboxRepository;

		@Autowired
		AllocationItemRepository allocationItemRepository;

		@Autowired
		private SkuRepository skuRepository;

		@Autowired
		private PlatformTransactionManager transactionManager;

		@Autowired
		private AllocationService allocationService; // à renommer selon ton vrai nom de classe

		private TransactionTemplate txTemplate;
		private OrderId orderId;

		@BeforeEach
		void setUp() {
			//txTemplate.execute(status -> {
				// 1. begin transaction
				// ... code métier
				// 2. commit (ou rollback si exception) IMMÉDIATEMENT à la fin du lambda
			//});



		}

		@AfterEach
		void tearDown() {
				orderRepository.deleteAll();
				skuRepository.deleteAll();
				orderOutboxRepository.deleteAll();
				allocationItemRepository.deleteAll();
		}


		private LineItemRequest createLineItemRequest(String productNr,int quantity,BigDecimal unitPrice){
			return new LineItemRequest(productNr, quantity, unitPrice);
		}

		private List<Sku> createAndSaveSku(){
		   return skuRepository.saveAll(List.of(
				Sku.create(new ProductNr("PROD01"), new Quantity(40), "WA-01"),
				Sku.create(new ProductNr("PROD02"), new Quantity(20), "WA-02"),
				Sku.create(new ProductNr("PROD03"), new Quantity(20), "WA-03")
			));
		}

		private List<CustomerOrder> createCustomerOrders() {
			LineItemRequest l1 = createLineItemRequest("PROD01", 40, new BigDecimal("10.00"));
			LineItemRequest l2 = createLineItemRequest("PROD02", 40, new BigDecimal("10.00"));
			LineItemRequest l3 = createLineItemRequest("PROD01", 30, new BigDecimal("10.00"));
			LineItemRequest l4 = createLineItemRequest("PROD02", 20, new BigDecimal("10.00"));
			LineItemRequest l5 = createLineItemRequest("PROD01", 30, new BigDecimal("10.00"));
			LineItemRequest l6 = createLineItemRequest("PROD02", 20, new BigDecimal("10.00"));

			CustomerOrder saveCustomerOrder = CustomerOrder.create(Priority.NORMAL, false, "EUR", List.of(l1, l2));
			CustomerOrder saveCustomerOrder02 = CustomerOrder.create(Priority.HIGH, true, "EUR", List.of(l3, l4));
			CustomerOrder saveCustomerOrder03 = CustomerOrder.create(Priority.HIGH, true, "EUR", List.of(l5, l6));
			return List.of(saveCustomerOrder, saveCustomerOrder02,saveCustomerOrder03);
		}




		private record OrderLineRequest(
			String orderLineItemId,
			String sku,
			int quantity,
			BigDecimal unitPrice,
			LineItemStatus status
		) {}

		private record OrderEventRequest(
			String orderId,
			String currency,
			BigDecimal totalAmount,
			String priority,
			boolean completeDeliveryRequired,
			List<CustomerOrderOptimisticLockingIT.OrderLineRequest> lines
		) {}

		private OrderReceivedEvent buildEvent(CustomerOrderOptimisticLockingIT.OrderEventRequest request) {
			List<OrderReceivedEvent.OrderLine> lines = request.lines().stream()
				.map(l -> OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(l.orderLineItemId())
					.sku(l.sku())
					.quantity(l.quantity())
					.unitPrice(l.unitPrice())
					.status(l.status())
					.build())
				.toList();

			return OrderReceivedEvent.builder()
				.eventId(UUID.randomUUID().toString())
				.orderId(request.orderId())
				.currency(request.currency())
				.totalAmount(request.totalAmount())
				.priority(request.priority())
				.completeDeliveryRequired(request.completeDeliveryRequired())
				.occurredAt(Instant.now())
				.lines(lines)
				.build();
		}


	@Test
	void concurrentReceiveOrder_thenCancelOne() throws Exception {
	    // create and save Sku
		createAndSaveSku();

		// saved tree Customer order parelelle
		LineItemRequest item01 = new LineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest item02 = new LineItemRequest("PROD02", 40, new BigDecimal("10.00"));
		LineItemRequest item03 = new LineItemRequest("PROD01", 30, new BigDecimal("10.00"));
		LineItemRequest item04 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));
		LineItemRequest item05 = new LineItemRequest("PROD01", 30, new BigDecimal("10.00"));
		LineItemRequest item06 = new LineItemRequest("PROD02", 20, new BigDecimal("10.00"));

		CreateOrderRequest createOrderRequest01 = new CreateOrderRequest(
			Priority.NORMAL, false, "EUR", List.of(item01, item02));

		CreateOrderRequest createOrderRequest02 = new CreateOrderRequest(
			Priority.HIGH, true, "EUR", List.of(item03, item04));

		CreateOrderRequest createOrderRequest03 = new CreateOrderRequest(
			Priority.HIGH, true, "EUR", List.of(item05, item06));

		ExecutorService executor = Executors.newFixedThreadPool(3);

		Callable<CreateOrderResponse> task01 = () -> orderService.receiveOrder(createOrderRequest01);
		Callable<CreateOrderResponse> task02 = () -> orderService.receiveOrder(createOrderRequest02);
		Callable<CreateOrderResponse> task03 = () -> orderService.receiveOrder(createOrderRequest03);

		// invokeAll bloque jusqu'à ce que les 3 tâches soient terminées
		List<Future<CreateOrderResponse>> futures = executor.invokeAll(List.of(task01, task02, task03));

		List<CreateOrderResponse> responses = new ArrayList<>();
		for (Future<CreateOrderResponse> future : futures) {
			responses.add(future.get(15, TimeUnit.SECONDS));
		}

		// ⚠️ shutdown supprimé ici — l'executor est réutilisé juste après

		long count = orderOutboxRepository.count();
		assertThat(count).isEqualTo(3);

		assertThat(responses).hasSize(3);
		responses.forEach(response -> assertThat(response.orderId()).isNotNull());

		// À ce stade, les 3 créations sont garanties terminées.
		List<CustomerOrder> saveOrders = orderRepository.findAll();

		// Après les 3 créations, AVANT d'appeler cancelOrder
		List<String> orderIds = saveOrders.stream()
			.map(order -> order.getId().toString() )
			.collect(Collectors.toList());

     // on attend que TOUTES les commandes soient sorties de l'état PENDING/en cours d'allocation
		orderIds.forEach(id ->
			await()
				.atMost(15, TimeUnit.SECONDS)
				.pollInterval(200, TimeUnit.MILLISECONDS)
				.untilAsserted(() -> {
					CustomerOrder order = orderRepository.findById(new OrderId(UUID.fromString(id))).orElseThrow();
					assertThat(order.getStatus()).isIn(
						OrderStatus.ALLOCATION_FAILED, // si ce statut existe chez vous
						OrderStatus.ALLOCATION_FAILED,
						OrderStatus.PARTIALLY_ALLOCATED
					);
				})
		);


		// On lance maintenant 2 annulations EN PARALLÈLE sur 2 commandes différentes.
		Callable<Void> cancelTask01 = () -> {
			orderService.cancelOrder(saveOrders.getFirst().getId(),
				new CancelOrderRequest("client01", "client request", CancellationSource.CUSTOMER_APP, null));
			return null;
		};

		Callable<Void> cancelTask02 = () -> {
			orderService.cancelOrder(saveOrders.get(1).getId(),
				new CancelOrderRequest("client01", "client request", CancellationSource.CUSTOMER_APP, null));
			return null;
		};

		List<Future<Void>> cancelFutures = executor.invokeAll(List.of(cancelTask01, cancelTask02));
		for (Future<Void> future : cancelFutures) {
			future.get(15, TimeUnit.SECONDS);
		}

		// shutdown appelé UNE SEULE FOIS, ici à la toute fin
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);

		// Vérifications
		CustomerOrder cancelledOrder1 = orderRepository.findById(saveOrders.get(0).getId()).orElseThrow();
		CustomerOrder cancelledOrder2 = orderRepository.findById(saveOrders.get(1).getId()).orElseThrow();
		CustomerOrder order3 = orderRepository.findById(saveOrders.get(2).getId()).orElseThrow();

		assertThat(cancelledOrder1.getStatus()).isEqualTo(OrderStatus.CANCELLED);
		assertThat(cancelledOrder2.getStatus()).isEqualTo(OrderStatus.CANCELLED);
		assertThat(order3.getStatus()).isEqualTo(OrderStatus.FULLY_ALLOCATED);
	}

}
*/

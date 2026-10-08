package com.stock.management.domain;

import com.stock.management.allocation.AllocationService;
import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemRepository;
import com.stock.management.kafka.event.OrderReceivedEvent;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.LineItemRequest;
import com.stock.management.sku.SkuRepository;
import com.stock.management.sku.SkuService;
import com.stock.management.sku.domain.Sku;
import com.stock.management.sku.domain.SkuId;
import com.stock.management.sku.domain.WarehouseLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.AssertionsForClassTypes.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest
@ActiveProfiles("test")
public class AllocationLockIT {
	@Autowired
	private AllocationService allocationService;

	// 🟢 Utiliser @SpyBean sur le service qui contient la requête SELECT ... FOR UPDATE
	@MockitoSpyBean
	private SkuService skuService;

	@Autowired
	AllocationItemRepository allocationItemRepository;

	@Autowired
	OrderService orderService;

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private SkuRepository skuRepository; // Pour initialiser les données réelles en BDD

	@AfterEach
	void cleanUp() {
		allocationItemRepository.deleteAll();
		orderRepository.deleteAll();
		skuRepository.deleteAll();
	}


	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void allocate_shouldPessimisticLockStock_andBlockConcurrentTransactions(boolean completeDeliveryRequired) throws Exception {
		// GIVEN : On prépare le stock en BDD

		LineItem lineItem01 = new LineItem(
			new LineItemId(UUID.fromString("00000000-0000-0000-0000-0000000000a1")),
			null,
			new ProductNr("SKU-1"),
			new Quantity(2),
			Quantity.zero(),
			new BigDecimal("200.0"),
			LineItemStatus.PENDING
		);

		LineItemRequest item01 = new LineItemRequest("SKU-1", 40, new BigDecimal(200.00));
		CustomerOrder savedOrderThread01 =  CustomerOrder.create(Priority.HIGH, completeDeliveryRequired, "EUR",List.of( item01));
		CustomerOrder savedOrderThread02 =  CustomerOrder.create(Priority.HIGH, completeDeliveryRequired, "EUR",List.of( item01));
		orderRepository.saveAll(List.of(savedOrderThread01,savedOrderThread02));


		Sku sku01 = Sku.create(new ProductNr("PROD01"),new Quantity(30),"WA-C01");
		Sku sku02 = Sku.create(new ProductNr("PROD02"),new Quantity(30),"WA-C02");
		skuRepository.saveAll(List.of(sku01,sku02));

		OrderReceivedEvent eventThread01 = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(savedOrderThread01.getId().toString())
			.currency("EUR")
			.totalAmount(new BigDecimal("800.00"))
			.priority("HIGH")
			.completeDeliveryRequired(completeDeliveryRequired
			)
			.occurredAt(Instant.now())
			.lines(List.of(
				OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(savedOrderThread01.getLineItems().getFirst().getId().toString())
					.sku("PROD01")
					.quantity(40)
					.unitPrice(new BigDecimal("200.00"))
					.status(LineItemStatus.PENDING)
					.build()
			))
			.build();

		OrderReceivedEvent eventThread02 = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(savedOrderThread02.getId().toString())
			.currency("EUR")
			.totalAmount(new BigDecimal("800.00"))
			.priority("HIGH")
			.completeDeliveryRequired(completeDeliveryRequired
			)
			.occurredAt(Instant.now())
			.lines(List.of(
				OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(savedOrderThread02.getLineItems().getFirst().getId().toString())
					.sku("PROD01")
					.quantity(40)
					.unitPrice(new BigDecimal("200.00"))
					.status(LineItemStatus.PENDING)
					.build()
			))
			.build();


		List<OrderReceivedEvent> eventsT01 = List.of(eventThread01);
		List<OrderReceivedEvent> eventsT02 = List.of(eventThread02);


		CountDownLatch lockAcquiredLatch = new CountDownLatch(1);
		CountDownLatch releaseLockLatch = new CountDownLatch(1);

		// Interception du verrou dans SkuService
		doAnswer(invocation -> {
			// ÉTAPE A : Le verrou est posé en BDD
			Object result = invocation.callRealMethod();

			// ÉTAPE B : On prévient le test (La transaction 1 est OUVERTE)
			//À l'Étape B (lockAcquiredLatch.countDown()) : Le Thread 1 est sur cette ligne.
			// La méthode @Transactional allocate() n'est absolument pas finie. Le COMMIT n'a pas eu lieu. La transaction est 100% active.
			// On prévient le thread principal du test : "C'est bon, le verrou BDD est posé, tu peux envoyer le Thread 2 !".
 			//
			lockAcquiredLatch.countDown();

			// ÉTAPE C : On met en PAUSE le Thread 1 à l'INTÉRIEUR de la transaction
			//À l'Étape C (releaseLockLatch.await(...)) : Le Thread 1 s'arrête d'avancer et attend.
			//Il garde la connexion JDBC ouverte et le verrou BDD actif.
			//"Mets-toi en pause à cet endroit précis. Attends que le thread principal fasse releaseLockLatch.countDown().
			// Mais si au bout de 5 secondes personne ne t'a réveillé, réveille-toi tout seul et passe à la ligne 4."
			releaseLockLatch.await(5, TimeUnit.SECONDS);
			return result;
		}).when(skuService).lockAndFetchAvailableStock(any()); // any ici vas recuperer l'argumet qu'on vas le passer via la vrai methode allocate()

		ExecutorService executor = Executors.newFixedThreadPool(2);

		// WHEN : Thread 1 exécute allocate() et bloque à l'intérieur de sa transaction
		Future<?> thread1Future = executor.submit(() -> allocationService.allocate(eventsT01));

		// Attente que le Thread 1 ait bien posé le verrou en BDD
		// sans ceci le thread2 ou principal ou qu'on veux tester vas s'executer en meme temp que le 1
		boolean lockAcquired = lockAcquiredLatch.await(2, TimeUnit.SECONDS);
		assertThat(lockAcquired).isTrue();

		// WHEN : Thread 2 tente d'exécuter allocate() sur le MÊME stock pendant que Thread 1 tient le verrou
		// Pendant que le Thread 1 attend à l'Étape C : Le Thread 2 essaie de faire la même chose et se fait bloquer par la BDD
		Future<?> thread2Future = executor.submit(() -> allocationService.allocate(eventsT02));

		// THEN : On vérifie que le Thread 2 est BLOQUÉ (Timeout) car la BDD refuse de lui donner les lignes Sku
		// si le timeout arrive on affiche une exception timeoutException
		assertThatThrownBy(() -> thread2Future.get(500, TimeUnit.MILLISECONDS))
			.isInstanceOf(TimeoutException.class);



		// CLEANUP : On libère le Thread 1 pour fermer sa transaction
		//Dès que cette ligne est exécutée, le Thread 1 sort de sa pause et reprend l'exécution de sa transaction donc quelle libere le thread on passe
		// directement a thread1Future.get(2, TimeUnit.SECONDS); et en meme temps en arriere plan le thread1 continu ses operation (en paralele)
 		//  raison pour la quelle on dit a test d'attendre 2 seconde dans  pour thread1Future.get(2, TimeUnit.SECONDS) laisser le thread1 commit ou rollback
 		// sans cela le test vas s'arreter immediatement sans que le thread1 n'est fini
		// donc il serra executer a la 501ms
		releaseLockLatch.countDown();

		// On attend la fin normale des 2 threads
		//Les méthodes de l'ExecutorService s'exécutent de manière asynchrone (en arrière-plan).
		//Si tu ne mets pas thread.get(), ton test JUnit pourrait se terminer immédiatement
		// après la ligne releaseLockLatch.countDown(), alors que les threads sont encore en train de s'exécuter en arrière-plan dans la JVM.
		//Le .get(2, TimeUnit.SECONDS) dit à JUnit : "Attends jusqu'à 2 secondes que ce thread ait complètement fini son travail avant de déclarer le test terminé".

		// 1. Thread 1 termine sa transaction et fait son COMMIT (libère le verrou BDD)
		//Spring fait le COMMIT SQL à la fin de la méthode transactionnelle dans le Thread 1
		//thread1Future.get() permet au test d'attendre que ce COMMIT
		thread1Future.get(2, TimeUnit.SECONDS);

		// 2. Thread 2, qui était bloqué, REPREND automatiquement et SE TERMINE SANS ERREUR !
		// Si la ligne ci-dessous passe sans TimeoutException ni execution exception,
		// cela prouve que Thread 2 a réussi son allocation dès que le verrou a été relâché.
		assertThatCode(() -> thread2Future.get(2, TimeUnit.SECONDS))
			.doesNotThrowAnyException();

		executor.shutdown();
	}


	@Test
	void allocate_shouldNotBlockConcurrentTransaction_whenOrdersUseDifferentSkus() throws Exception {

		LineItemRequest itemThread01 = new LineItemRequest("PRD01", 40, new BigDecimal(200.00));
		LineItemRequest itemThread02 = new LineItemRequest("PRD02", 40, new BigDecimal(200.00));
		CustomerOrder savedOrderThread01 =  CustomerOrder.create(Priority.HIGH, true, "EUR",List.of( itemThread01));
		CustomerOrder savedOrderThread02 =  CustomerOrder.create(Priority.HIGH, true, "EUR",List.of( itemThread02));

		orderRepository.saveAll(List.of(savedOrderThread01,savedOrderThread02) );
		// 1. Données pour le THREAD 1
		OrderReceivedEvent eventThread1 = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(savedOrderThread01.getId().toString())
			.currency("EUR")
			.totalAmount(new BigDecimal("200.00"))
			.priority("HIGH")
			.completeDeliveryRequired(false)
			.occurredAt(Instant.now())
			.lines(List.of(
				OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(savedOrderThread01.getLineItems().getFirst().getId().toString())
					.sku("PRD01")
					.quantity(40)
					.unitPrice(new BigDecimal("200.00"))
					.status(LineItemStatus.PENDING)
					.build()
			))
			.build();

		// 2. Données pour le THREAD 2 (Différentes)
		OrderReceivedEvent eventThread2 = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(savedOrderThread02.getId().toString())
			.currency("EUR")
			.totalAmount(new BigDecimal("600.00"))
			.priority("HIGH")
			.completeDeliveryRequired(false)
			.occurredAt(Instant.now())
			.lines(List.of(
				OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(savedOrderThread02.getLineItems().getFirst().getId().toString())
					.sku("PRD02")
					.quantity(40)
					.unitPrice(new BigDecimal("200.00"))
					.status(LineItemStatus.PENDING)
					.build()
			))
			.build();



		Sku sku01 = Sku.create(new ProductNr("PRD01"),new Quantity(30),"WA-C01");
		Sku sku02 = Sku.create(new ProductNr("PRD02"),new Quantity(30),"WA-C02");
		skuRepository.saveAll(List.of(sku01,sku02));

		CountDownLatch lockAcquiredLatch = new CountDownLatch(1);
		CountDownLatch releaseLockLatch = new CountDownLatch(1);

		// Ne retarder QUE le premier appel : sinon thread2 se bloque aussi sur ce même
		// stub et le test échoue systématiquement, indépendamment du code testé.
		AtomicBoolean firstCallIntercepted = new AtomicBoolean(true);

		doAnswer(invocation -> {
			Object result = invocation.callRealMethod();

			if (firstCallIntercepted.compareAndSet(true, false)) {
				lockAcquiredLatch.countDown();
				releaseLockLatch.await(5, TimeUnit.SECONDS);
			}
			return result;
		}).when(skuService).lockAndFetchAvailableStock(any());

		ExecutorService executor = Executors.newFixedThreadPool(2);

		try {
			// WHEN : Lancement du Thread 1 avec ses propres données
			Future<?> thread1Future = executor.submit(() -> allocationService.allocate(List.of(eventThread1)));

			boolean lockAcquired = lockAcquiredLatch.await(2, TimeUnit.SECONDS);
			assertThat(lockAcquired).isTrue();

			// WHEN : Lancement du Thread 2 avec des données différentes
			Future<?> thread2Future = executor.submit(() -> allocationService.allocate(List.of(eventThread2)));

			// THEN : Thread 2 doit SE TERMINER AVEC SUCCÈS SANS ATTENDRE (Aucun blocage)
			assertThatCode(() -> thread2Future.get(2, TimeUnit.SECONDS))
				.doesNotThrowAnyException();

			// CLEANUP
			releaseLockLatch.countDown();
			thread1Future.get(2, TimeUnit.SECONDS);

		} finally {
			releaseLockLatch.countDown();
			executor.shutdownNow();
		}
	}

	private List<CustomerOrder> createCustomerOrders() {
		LineItemRequest l1 = createLineItemRequest("PROD01", 40, new BigDecimal("10.00"));
		LineItemRequest l2 = createLineItemRequest("PROD02", 40, new BigDecimal("10.00"));
		LineItemRequest l3 = createLineItemRequest("PROD01", 30, new BigDecimal("10.00"));
		LineItemRequest l4 = createLineItemRequest("PROD02", 20, new BigDecimal("10.00"));

		CustomerOrder saveCustomerOrder = CustomerOrder.create(Priority.NORMAL, false, "EUR", List.of(l1, l2));
		CustomerOrder saveCustomerOrder02 = CustomerOrder.create(Priority.HIGH, true, "EUR", List.of(l3, l4));

		return List.of(saveCustomerOrder, saveCustomerOrder02);
	}


	private void assertStockLevels(List<Sku> skuList) {
		assertThat(((Sku) skuList.get(0)).getAvailableQuantity().getValue()).isZero();
		assertThat(((Sku) skuList.get(1)).getAvailableQuantity().getValue()).isZero();
		assertThat(((Sku) skuList.get(2)).getAvailableQuantity().getValue()).isEqualTo(20);
	}


	public List<OrderReceivedEvent> buildOrderEvents(List<CustomerOrder> orders, List<LineItem> lineItems) {
		OrderReceivedEvent event = buildEvent(new OrderEventRequest(
			orders.get(0).getId().toString(),
			"EUR",
			new BigDecimal("800.00"),
			"NORMAL",
			false,
			List.of(
				new OrderLineRequest(lineItems.get(0).getId().toString(), "PROD01", 40, new BigDecimal("10.00"), LineItemStatus.PENDING),
				new OrderLineRequest(lineItems.get(1).getId().toString(), "PROD02", 40, new BigDecimal("10.00"), LineItemStatus.PENDING)
			)
		));

		OrderReceivedEvent event02 = buildEvent(new OrderEventRequest(
			orders.get(1).getId().toString(),
			"EUR",
			new BigDecimal("800.00"),
			"NORMAL",
			true,
			List.of(
				new OrderLineRequest(lineItems.get(2).getId().toString(), "PROD01", 30, new BigDecimal("10.00"), LineItemStatus.PENDING),
				new OrderLineRequest(lineItems.get(3).getId().toString(), "PROD02", 20, new BigDecimal("10.00"), LineItemStatus.PENDING)
			)
		));

		return List.of(event, event02);
	}

	@Test
	void allocate_insufficientStockOnBothLines_updatesOrderStockAndAllocationItems() {
		// GIVEN : commande à livraison partielle autorisée, demande > stock sur les 2 lignes
		List<CustomerOrder> customerOrders = createCustomerOrders();

		List<CustomerOrder> orders = orderRepository.saveAll(
			customerOrders
		);
		skuRepository.saveAll(List.of(
			Sku.create(new ProductNr("PROD01"), new Quantity(30), "WA-01"),
			Sku.create(new ProductNr("PROD02"), new Quantity(20), "WA-02"),
			Sku.create(new ProductNr("PROD03"), new Quantity(20), "WA-03")
		));

		LineItem lineItem1 = orders.get(0).getLineItems().get(0);
		LineItem lineItem2 = orders.get(0).getLineItems().get(1);
		LineItem lineItem3 = orders.get(1).getLineItems().get(0);
		LineItem lineItem4 = orders.get(1).getLineItems().get(1);


		OrderReceivedEvent event = buildEvent(new OrderEventRequest(
			orders.get(0).getId().toString(),
			"EUR",
			new BigDecimal("800.00"),
			"NORMAL",
			false,
			List.of(
				new OrderLineRequest(lineItem1.getId().toString(), "PROD01", 40, new BigDecimal("10.00"), LineItemStatus.PENDING),
				new OrderLineRequest(lineItem2.getId().toString(), "PROD02", 40, new BigDecimal("10.00"), LineItemStatus.PENDING)
			)
		));

		OrderReceivedEvent event02 = buildEvent(new OrderEventRequest(
			orders.get(1).getId().toString(),
			"EUR",
			new BigDecimal("800.00"),
			"NORMAL",
			true,
			List.of(
				new OrderLineRequest(lineItem3.getId().toString(), "PROD01", 30, new BigDecimal("10.00"), LineItemStatus.PENDING),
				new OrderLineRequest(lineItem4.getId().toString(), "PROD02", 20, new BigDecimal("10.00"), LineItemStatus.PENDING)
			)
		));

		// WHEN
		allocationService.allocate(List.of(event,event02));

		List<CustomerOrder> loadCustomer = orderRepository.findAllByIdsWithLineItems(List.of(orders.get(0).getId(),orders.get(1).getId()));

		List<LineItem> lineItems01 = loadCustomer.get(0).getLineItems();
		List<LineItem> lineItems02 = loadCustomer.get(1).getLineItems();

		assertThat(loadCustomer.get(0).getStatus()).isEqualTo(OrderStatus.PARTIALLY_ALLOCATED);
		assertThat(loadCustomer.get(1).getStatus()).isEqualTo(OrderStatus.ALLOCATION_FAILED);

        assertLineItemStatuses(lineItems01,lineItems02);

		List<Sku>skuList = skuRepository.findAll();

		// THEN : stock totalement consommé sur les deux SKU
		assertStockLevels(skuList);

		// THEN : allocationItem — 1 ligne "allouée" + 1 ligne "manquante" par ligne de commande
		List<AllocationItem> items = allocationItemRepository.findAlreadyAllocatedOrders(
			         List.of(UUID.fromString(orders.get(1).getId().toString())));

		assertThat(items.size()).isEqualTo(2);

		//assertAllocatedAndMissing(items, lineItem3.getId(), 30, 10); // PROD01 : 30 alloué, 10 manquant
		//assertAllocatedAndMissing(items, lineItem4.getId(), 20, 20); // PROD02 : 20 alloué, 20 manquant
		assertAllocated(items, lineItem3.getId(), 30); // PROD01 : 30 alloué
		assertAllocated(items, lineItem4.getId(), 20); // PROD02 : 20 alloué
	}

	private void assertLineItemStatuses(List<LineItem> lineItems01, List<LineItem> lineItems02) {
		for (LineItem item : lineItems01) {
			System.out.println("itemStatusAllo:" + item.getStatus());
			assertThat(item.getStatus()).isEqualTo(LineItemStatus.NOT_ALLOCATED);
		}

		for (LineItem item : lineItems02) {
			assertThat(item.getStatus()).isEqualTo(LineItemStatus.FULLY_ALLOCATED);
		}
	}

	private OrderReceivedEvent.OrderLine orderLine(LineItem lineItem, String sku, int qty) {
		return OrderReceivedEvent.OrderLine.builder()
			.orderLineItemId(lineItem.getId().toString())
			.sku(sku)
			.quantity(qty)
			.unitPrice(new BigDecimal("10.00"))
			.status(LineItemStatus.PENDING)
			.build();
	}

	private void assertAllocatedAndMissing(List<AllocationItem> items, LineItemId lineId, int expectedAllocated, int expectedMissing) {
		List<AllocationItem> forLine = items.stream()
			.filter(item -> item.getLineItemId().equals(lineId))
			.toList();
		int i = 0;
		assertThat(forLine.size()).isEqualTo(2);
		for(AllocationItem item : forLine){

			if(i == 0){
				i++;
				assertThat(item.getQuantity()).isEqualTo(expectedAllocated);
				assertThat(item.getRemainingQuantity()).isZero();
			}
			assertThat(item.getQuantity()).isZero();
			assertThat(item.getRemainingQuantity()).isEqualTo(expectedMissing);
		}
	}

	private void assertAllocated(List<AllocationItem> items, LineItemId lineId, int expectedAllocated) {
		List<AllocationItem> forLine = items.stream()
			.filter(item -> item.getLineItemId().equals(UUID.fromString(lineId.getValue().toString())))
			.toList();

		assertThat(forLine.size()).isEqualTo(1);
		for(AllocationItem item : forLine){

				assertThat(item.getQuantity()).isEqualTo(expectedAllocated);
				assertThat(item.getRemainingQuantity()).isZero();
			}
	}

	private LineItemRequest createLineItemRequest(String productNr,int quantity,BigDecimal unitPrice){
		return new LineItemRequest(productNr, quantity, unitPrice);
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
		List<OrderLineRequest> lines
	) {}

	private OrderReceivedEvent buildEvent(OrderEventRequest request) {
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




}

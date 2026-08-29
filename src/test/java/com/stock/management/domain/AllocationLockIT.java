package com.stock.management.domain;

import com.stock.management.allocation.AllocationService;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;


public class AllocationLockIT {
	@Autowired
	private AllocationService allocationService;

	// 🟢 Utiliser @SpyBean sur le service qui contient la requête SELECT ... FOR UPDATE
	@MockitoSpyBean
	private SkuService skuService;

	@Autowired
	OrderService orderService;

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private SkuRepository skuRepository; // Pour initialiser les données réelles en BDD

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

		LineItemRequest item01 = new LineItemRequest("SKU-1", 40, new BigDecimal(200.0));
		CustomerOrder savedOrder =  CustomerOrder.create(Priority.HIGH, completeDeliveryRequired, "EUR",List.of( item01));
		orderRepository.save(savedOrder);


		Sku sku01 = Sku.create(new ProductNr("Sku-1"),new Quantity(30),"WA-C01");
		Sku sku02 = Sku.create(new ProductNr("Sku-1"),new Quantity(30),"WA-C02");
		skuRepository.saveAll(List.of(sku01,sku02));

		OrderReceivedEvent event = OrderReceivedEvent.builder()
			.eventId(UUID.randomUUID().toString())
			.orderId(savedOrder.getId().toString())
			.currency("EUR")
			.totalAmount(new BigDecimal("600.00"))
			.priority("HIGH")
			.completeDeliveryRequired(true)
			.occurredAt(Instant.now())
			.lines(List.of(
				OrderReceivedEvent.OrderLine.builder()
					.orderLineItemId(savedOrder.getLineItems().getFirst().getId().toString())
					.sku("SKU-1")
					.quantity(40)
					.unitPrice(new BigDecimal("200.00"))
					.status(LineItemStatus.PENDING)
					.build()
			))
			.build();


		List<OrderReceivedEvent> events = List.of(event);

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
		Future<?> thread1Future = executor.submit(() -> allocationService.allocate(events));

		// Attente que le Thread 1 ait bien posé le verrou en BDD
		// sans ceci le thread2 ou principal ou qu'on veux tester vas s'executer en meme temp que le 1
		boolean lockAcquired = lockAcquiredLatch.await(2, TimeUnit.SECONDS);
		assertThat(lockAcquired).isTrue();

		// WHEN : Thread 2 tente d'exécuter allocate() sur le MÊME stock pendant que Thread 1 tient le verrou
		// Pendant que le Thread 1 attend à l'Étape C : Le Thread 2 essaie de faire la même chose et se fait bloquer par la BDD
		Future<?> thread2Future = executor.submit(() -> allocationService.allocate(events));

		// THEN : On vérifie que le Thread 2 est BLOQUÉ (Timeout) car la BDD refuse de lui donner les lignes Sku
		// si le timeout arrive on affiche une exception timeoutException
		assertThatThrownBy(() -> thread2Future.get(500, TimeUnit.MILLISECONDS))
			.isInstanceOf(TimeoutException.class);



		// CLEANUP : On libère le Thread 1 pour fermer sa transaction
		//Dès que cette ligne est exécutée, le Thread 1 sort de sa pause et reprend l'exécution de sa transaction donc quelle libere le thread on passe
		// directement a thread1Future.get(2, TimeUnit.SECONDS); et en meme temps en arriere plan le thread1 continu ses operation (en paralele)
 		//  raison pour la quelle on dit a test d'attendre 2 seconde dans  pour thread1Future.get(2, TimeUnit.SECONDS) laisser le thread1 commit ou rollback
 		// sans cela le test vas s'arreter immediatement sans que le thread1 n'est fini
		releaseLockLatch.countDown();

		// On attend la fin normale des 2 threads
		//Les méthodes de l'ExecutorService s'exécutent de manière asynchrone (en arrière-plan).
		//Si tu ne mets pas thread.get(), ton test JUnit pourrait se terminer immédiatement
		// après la ligne releaseLockLatch.countDown(), alors que les threads sont encore en train de s'exécuter en arrière-plan dans la JVM.
		//Le .get(2, TimeUnit.SECONDS) dit à JUnit : "Attends jusqu'à 2 secondes que ce thread ait complètement fini son travail avant de déclarer le test terminé".

		thread1Future.get(2, TimeUnit.SECONDS);
		thread2Future.get(2, TimeUnit.SECONDS);

		executor.shutdown();
	}

}

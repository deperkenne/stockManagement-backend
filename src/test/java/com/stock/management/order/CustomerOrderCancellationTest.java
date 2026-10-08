package com.stock.management.order;

import com.stock.management.allocation.AllocationReleasedEvent;
import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CancelOrderRequest;
import com.stock.management.order.dto.CancelOrderResponse;
import com.stock.management.sku.SkuService;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CustomerOrderCancellationTest {

	@Mock
	private OrderRepository orderRepository;

	@Mock private AllocationItemService allocationItemService;

	@Mock private SkuService skuService;

	@Mock private  ApplicationEventPublisher eventPublisher;

	@Mock
	private OrderStatusHistoryRepository orderStatusHistoryRepository;

	// Si releaseStockForPartialOrder et releaseStockAndRetryPending sont dans le même service,
	// on peut utiliser @InjectMocks. Si ce sont des dépendances, il faudrait les mocker aussi.
	@InjectMocks
	private OrderService orderService;

	@Test
	void quandOrderIdNull_alorsLanceIllegalArgumentException() {
		// Given
		CancelOrderRequest request = new CancelOrderRequest("Reason",  "User",CancellationSource.CUSTOMER_APP,null);

		// When & Then
		assertThrows(IllegalArgumentException.class, () -> {
			orderService.cancelOrder(null, request);
		});
	}

	@Test
	void quandCommandeNon_trouvee_alorsLanceEntityNotFoundException() {
		// Given
		OrderId orderId = new OrderId(UUID.randomUUID());
		CancelOrderRequest request = new CancelOrderRequest("Reason",  "User",CancellationSource.CUSTOMER_APP,null);

		when(orderRepository.findByIdWithLineItemsForUpdate(orderId)).thenReturn(Optional.empty());

		// When & Then
		assertThrows(EntityNotFoundException.class, () -> {
			orderService.cancelOrder(orderId, request);
		});
	}

	@Test
	void quandCommandeStandard_alorsAnnuleEtSauvegardeHistoire() {
		// Given
		OrderId orderId = new OrderId(UUID.randomUUID());
		CancelOrderRequest request = new CancelOrderRequest("Change avis",  "User",CancellationSource.CUSTOMER_APP,null);

		CustomerOrder orderMock = mock(CustomerOrder.class);
		when(orderMock.getStatus()).thenReturn(OrderStatus.ALLOCATION_FAILED);
		when(orderRepository.findByIdWithLineItemsForUpdate(orderId)).thenReturn(Optional.of(orderMock));

		// When
		CancelOrderResponse response = orderService.cancelOrder(orderId, request);

		// Then
		assertNotNull(response);
		assertEquals(orderId.toString(), response.orderId());
		assertEquals("CANCELLED", response.status());

		// Vérifications des appels
		verify(orderMock, times(1)).validateCancellationEligibility();
		verify(orderMock, times(1)).cancel(request.cancellationSource(), request.reason(), request.cancelledBy());
		verify(orderStatusHistoryRepository, times(1)).save(any(OrderStatusHistory.class));
	}


	@Test
	void shouldReleaseStockAndPublishRetryEventWhenCancellingPartiallyAllocatedOrder() {
		// Given
		OrderId orderId = new OrderId(UUID.randomUUID());
		CancelOrderRequest request = new CancelOrderRequest("Change avis",  "User",CancellationSource.CUSTOMER_APP,null);


		// Mock de la commande et de son état initial
		CustomerOrder orderMock = mock(CustomerOrder.class);
		when(orderMock.getStatus()).thenReturn(OrderStatus.PARTIALLY_ALLOCATED);
		when(orderMock.getId()).thenReturn(orderId);
		when(orderMock.extractLineItemUuids()).thenReturn(List.of(UUID.randomUUID()));
		when(orderRepository.findByIdWithLineItemsForUpdate(orderId)).thenReturn(Optional.of(orderMock));

		// Mock des éléments d'allocation actifs liés à la commande
		AllocationItem allocationItemMock = mock(AllocationItem.class);
		when(allocationItemService.findAllByLineItemIdIn(anyList()))
			.thenReturn(List.of(allocationItemMock));

		// When : Exécution de l'annulation de la commande
		CancelOrderResponse response = orderService.cancelOrder(orderId, request);

		// Then : Vérifications globales de la réponse
		assertNotNull(response);
		assertEquals(orderId.toString(), response.orderId());
		assertEquals("CANCELLED", response.status());

		// 1. Vérification du cycle de vie de la commande
		verify(orderMock, times(1)).validateCancellationEligibility();
		verify(orderMock, times(1)).cancel(request.cancellationSource(), request.reason(), request.cancelledBy());

		// 2. Vérification de la mutation des AllocationItems (Domain-Driven Design)
		verify(allocationItemMock, times(1)).cancel();

		// 3. Vérification de la libération physique du stock en masse
		verify(skuService, times(1)).releaseBulkStock(anyList());

		// 4. Vérification de la publication de l'événement asynchrone pour le retry
		verify(eventPublisher, times(1)).publishEvent(any(AllocationReleasedEvent.class));

		// 5. Vérification de la traçabilité (Historique des statuts)
		verify(orderStatusHistoryRepository, times(1)).save(any(OrderStatusHistory.class));
	}

}

package com.stock.management.domain;

import com.stock.management.order.domain.*;
import com.stock.management.order.dto.LineItemRequest;
import jakarta.persistence.criteria.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static com.stock.management.order.domain.LineItemStatus.*;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
// Syntaxe : assertEquals(LineItemStatus.CANCELLED, cancelledItem.getStatus());
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;
import static scala.reflect.io.NoAbstractFile.foreach;

public class CustomerOrderTest {
	private CustomerOrder order;
	private List<LineItemId> ids;
	private List<UUID> uuids;

	@BeforeEach
	void setUp() {

		 ids = List.of(
			 new LineItemId(UUID.fromString("11111111-1111-1111-1111-111111111111")),
			 new LineItemId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
		);

		uuids = List.of(
			UUID.fromString("11111111-1111-1111-1111-111111111111"),
			UUID.fromString("22222222-2222-2222-2222-222222222222"),
			UUID.fromString("33333333-2222-2222-2222-222222222222")
		);
	}


	@Test
	void cancelLines_shouldCancelSpecifiedLineItems() {
		//GIVEN
		order = createMockCustomerOrder(PENDING,PENDING,PENDING);
		// SUT
		order.cancelLines(ids);
		// THEN 1 : Vérifier que les 2 lignes ciblées sont bien CANCELLED
		List<LineItem> cancelledItems = order.getLineItems().stream()
			.filter(line -> ids.contains(line.getId()))
			.toList();

		assertEquals(2, cancelledItems.size());
		for (LineItem item : cancelledItems) {
			assertEquals(CANCELLED, item.getStatus());
		}

        // THEN 2 : Isoler la/les ligne(s) restante(s) (non ciblées par l'annulation)
		List<LineItem> remainingItems = order.getLineItems().stream()
			.filter(line -> !ids.contains(line.getId()))
			.toList();

		assertEquals(1, remainingItems.size());

		for (LineItem item : remainingItems) {
			assertEquals(LineItemStatus.PENDING, item.getStatus(),"error sur cancel");
		}
	}

	// -------------------------------------------------------------------------
	// 2. CAS D'ERREURS ET D'EXCEPTIONS (EDGE & FAILURE CASES)
	// -------------------------------------------------------------------------

	@Test
	void cancelLines_shouldThrowException_whenTargetsIsNull() {
		//GIVEN
		order = createMockCustomerOrder(PENDING,PENDING,PENDING);

		assertThatThrownBy(() -> order.cancelLines(null))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("cannot be null or empty");
	}


	@Test
	void cancelLines_shouldHandleDuplicatesInTargetList() {
		//GIVEN
		order = createMockCustomerOrder(PENDING,PENDING,PENDING);
		LineItemId targetId = ids.get(0);

		// ACT : Passage de doublons dans la liste
		order.cancelLines(ids);

		// ASSERT : La ligne ciblée est bien annulée
		List<LineItem> cancelledItems = order.getLineItems().stream()
			.filter(line -> line.getId().equals(targetId))
			.toList();

		// ASSERT : Grâce à Set.copyOf, l'annulation réussit sans lever d'exception
		assertEquals(1,cancelledItems.size());
	}

	@Test
	void cancelLines_shouldThrowException_whenTargetsIsEmpty() {
		//GIVEN
		order = createMockCustomerOrder(PENDING,PENDING,PENDING);
		assertThatThrownBy(() -> order.cancelLines(Collections.emptyList()))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("cannot be null or empty");
	}


	@Test
	void emptyLineItemsList_shouldThrowIllegalArgumentException() {
		CustomerOrder order = new CustomerOrder(
			new OrderId(UUID.fromString("00000000-0000-0000-0000-0000000000a1")),null,null,
			List.of()  // liste vide
		);

		assertThatThrownBy(order::evaluateAndModifyGlobalStatus)
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("order muss habe least one lineitem");
	}

	@ParameterizedTest(name = "{index}: [{0}, {1}, {2}] => exception attendue")
	@MethodSource("nullStatusPositions")
	void nullStatusInMiddle_shouldThrow(LineItemStatus status1, LineItemStatus status2, LineItemStatus status3,String expectedMessage) {
		//GIVEN
		CustomerOrder order = createMockCustomerOrder(status1, status2,status3);

		assertThatThrownBy(order::evaluateAndModifyGlobalStatus)
			.isInstanceOf(IllegalArgumentException.class)
		    .hasMessage(expectedMessage);
	}

	static Stream<Arguments> nullStatusPositions() {
		String expectedMsg = "item muss contain a status"; // aligné avec le code réel
		return Stream.of(
			arguments(null, CANCELLED, CANCELLED, expectedMsg),
			arguments(FULLY_ALLOCATED, null, FULLY_ALLOCATED, expectedMsg),
			arguments(FULLY_ALLOCATED, FULLY_ALLOCATED, null, expectedMsg)
		);
	}





	@ParameterizedTest(name = "{index}: [{0}, {1}, {2}] => {3}")
	@MethodSource("statusCombinations")
	void evaluateGlobalStatus_coversAllStateTransitions(
		LineItemStatus status1, LineItemStatus status2, LineItemStatus status3,
		OrderStatus expected) {

		CustomerOrder order = createMockCustomerOrder(status1, status2, status3);

		order.evaluateAndModifyGlobalStatus();
        assertEquals(expected, order.getStatus(),"order not correspond");
	}



	@Test
	void requestedUuidsMatchingNonCancelledLines_shouldReturnTheirIds() {
		//GIVEN
		order = createMockCustomerOrder(CANCELLED,PENDING,PENDING);
		// SUT
		List<LineItemId> result = order.extractEligibleLineIdsForCancellation(
			uuids
		);

		UUID expectedId02 = UUID.fromString("33333333-2222-2222-2222-222222222222");
		UUID expectedId01 = UUID.fromString("22222222-2222-2222-2222-222222222222");

		assertEquals(2,result.size(),"list muss contain only two IDs");
		assertEquals(expectedId01,UUID.fromString(result.get(0).toString()),"not correct id had been inserted");
		assertEquals(expectedId02,UUID.fromString(result.get(1).toString()),"not correct id had been inserted");

	}

	@Test
	void nullLineItemsOnOrder_shouldNotThrowOrShouldThrowExplicitly() {
		order = new CustomerOrder(new OrderId(UUID.fromString("33333333-2222-2222-2222-222222222222")),null,null,List.of());
		// SUT
		assertThatThrownBy(() -> order.extractEligibleLineIdsForCancellation(uuids))
			.isInstanceOf(NullPointerException.class)
			.hasMessage("item muss not be null or empty");
	}

	static Stream<Arguments> statusCombinations() {
		return Stream.of(
			arguments(CANCELLED, CANCELLED, CANCELLED, OrderStatus.CANCELLED),
			arguments(FULLY_ALLOCATED, FULLY_ALLOCATED, FULLY_ALLOCATED, OrderStatus.FULLY_ALLOCATED),
			arguments(FULLY_ALLOCATED, FULLY_ALLOCATED, CANCELLED, OrderStatus.FULLY_ALLOCATED),
			arguments(PARTIALLY_ALLOCATED, PENDING, PENDING, OrderStatus.PARTIALLY_ALLOCATED),
			arguments(FULLY_ALLOCATED, PENDING, PENDING, OrderStatus.PARTIALLY_ALLOCATED),
			arguments(PENDING, PENDING, PENDING, OrderStatus.PENDING),
			arguments(CANCELLED, PENDING, PENDING, OrderStatus.PENDING),        // ← régression bug
			arguments(CANCELLED, PARTIALLY_ALLOCATED, PENDING, OrderStatus.PARTIALLY_ALLOCATED)
		);
	}

	private CustomerOrder createMockCustomerOrder (LineItemStatus status1, LineItemStatus status2,LineItemStatus status3){
		LineItem item1 = createMockLineItem("11111111-1111-1111-1111-111111111111",null,"PROD01",status1,300,"30.0");
		LineItem item2 = createMockLineItem("22222222-2222-2222-2222-222222222222",null,"PROD02",status2,200,"20.0");
		LineItem item3 = createMockLineItem("33333333-2222-2222-2222-222222222222",null,"PROD03",status3,200,"20.0");
		return new CustomerOrder(new OrderId(UUID.fromString("00000000-0000-0000-0000-0000000000a1")),null,null,List.of(item1,item2,item3));
	}



	private List<LineItemRequest> createMockLineItemRequests() {
		LineItemRequest request1 = mock(LineItemRequest.class);
		LineItemRequest request2 = mock(LineItemRequest.class);

		when(request1.productNr()).thenReturn("PRN01");
		when(request1.requestedQty()).thenReturn(100);
		when(request1.unitPrice()).thenReturn(new BigDecimal("20.0"));

		when(request2.productNr()).thenReturn("PRN02");
		when(request2.requestedQty()).thenReturn(50);
		when(request2.unitPrice()).thenReturn(new BigDecimal("15.5"));

		return List.of(request1, request2);
	}


	private void configureCancelBehavior(LineItem mockItem){
		doAnswer(invocation -> {
			when(mockItem.getStatus()).thenReturn(CANCELLED);
			return null;
		}).when(mockItem).cancel();
	}


	private LineItem  createMockLineItem(
		String id,
		CustomerOrder order,
		String productNr,
		LineItemStatus status,
		int quantity,
		String price){
		LineItem item = mock(LineItem.class);

		when(item.getId()).thenReturn(new LineItemId(UUID.fromString(id)));
		when(item.getCustomerOrder()).thenReturn(null);
		when(item.getProductNr()).thenReturn(new ProductNr(productNr));
		when(item.getStatus()).thenReturn(status);
		when(item.getRequestedQty()).thenReturn(new Quantity(quantity));
		when(item.getUnitPrice()).thenReturn(new BigDecimal(price));

		configureCancelBehavior(item);
		return item;

	}





	// --- Bug #1 : lineItems null (documente le bug tant que non fixé) ---
	@Test
	void nullLineItems_shouldThrowIllegalArgumentException_notNpe() {
		order = new CustomerOrder(new OrderId(UUID.fromString("33333333-2222-2222-2222-222222222222")),null,null,List.of());

		// Ce test échouera avec une NPE tant que isEmptyOrNull() n'est pas
		// utilisé dans un if — il DOIT lever IllegalArgumentException, pas NPE
		assertThatThrownBy(() -> order.cancel(CancellationSource.CUSTOMER_APP, "changed mind", "user123"))
			.isNotInstanceOf(NullPointerException.class)
			.hasMessage("list item muss not be null or empty");
	}

	// --- Garde existante : FULLY_ALLOCATED ---
	@ParameterizedTest(name = "{index}: status={0} => order.getStatus()={0}")
	@MethodSource("explicitStatuses")
	void fullyAllocatedOrder_shouldThrowIllegalStateException(OrderStatus status) {
		CustomerOrder order = new CustomerOrder(new OrderId(UUID.fromString("33333333-2222-2222-2222-222222222222")),status,null,List.of());

		assertThatThrownBy(() -> order.cancel(CancellationSource.CUSTOMER_APP, "no reason", "user123"))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Cannot cancel a completed order");
	}
	static Stream<Arguments> explicitStatuses() {
		return Stream.of(
			arguments(OrderStatus.CANCELLED),
			arguments(OrderStatus.FULLY_ALLOCATED)
		);
	}

	// --- Cas nominal ---
	@Test
	void pendingOrder_shouldBeCancelledWithAllFieldsSet() {
		CustomerOrder order = new CustomerOrder(new OrderId(UUID.fromString("33333333-2222-2222-2222-222222222222")),OrderStatus.PENDING,null,List.of());

		Instant before = Instant.now();
		order.cancel(CancellationSource.CUSTOMER_APP, "changed mind", "user123");
		Instant after = Instant.now();

		assertEquals( OrderStatus.CANCELLED ,order.getStatus());
		assertEquals("changed mind",order.getCancelReason());
		assertEquals("user123",order.getCancelBy());
		assertEquals(CancellationSource.CUSTOMER_APP,order.getCancellationSource());
	}


	private CustomerOrder buildOrderWithLineItems(LineItem lineItem, OrderStatus status) {
		return new CustomerOrder(new OrderId(UUID.fromString("33333333-2222-2222-2222-222222222222")), status,null, List.of(lineItem));
	}

	// --- Mix de statuts de lignes ---
	@Test
	void mixedLineItemStatuses_onlyNonCancelledLinesAreCancelled() {
		LineItem item1 = createMockLineItem("11111111-1111-1111-1111-111111111111", null,
			"PROD01", LineItemStatus.PENDING, 100, "10.0");
		LineItem item2 = createMockLineItem("22222222-2222-2222-2222-222222222222", null,
			"PROD02", LineItemStatus.CANCELLED, 200, "20.0");
		LineItem item3 = createMockLineItem("33333333-2222-2222-2222-222222222222", null,
			"PROD03", LineItemStatus.PARTIALLY_ALLOCATED, 200, "20.0");
		List<LineItem> lineItems = List.of(item1,item2,item3);
		CustomerOrder order = new CustomerOrder(new OrderId(UUID.fromString("33333333-2222-2222-2222-222222222222")),OrderStatus.PENDING,null,lineItems );

		order.cancel(CancellationSource.CUSTOMER_APP, "reason", "user123");
		for(LineItem item : lineItems){
            assertEquals(CANCELLED,item.getStatus());
		}
	}

}

package com.stock.management.domain;

import com.stock.management.order.InvalidOrderStateException;
import com.stock.management.order.LineItemNotFoundException;
import com.stock.management.order.OrderCancellationException;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.LineItemRequest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.stock.management.order.domain.LineItemStatus.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests unitaires de l'agrégat CustomerOrder, sans mock : de vraies commandes et de vraies lignes.
 * Une classe @Nested par méthode publique utilisée en production.
 */
@DisplayName("CustomerOrder")
class CustomerOrderTest {

	private static final OrderId ORDER_ID = new OrderId(UUID.fromString("00000000-0000-0000-0000-0000000000a1"));
	private static final UUID LINE_1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
	private static final UUID LINE_2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
	private static final UUID LINE_3 = UUID.fromString("33333333-3333-3333-3333-333333333333");
	private static final UUID UNKNOWN_LINE = UUID.fromString("99999999-9999-9999-9999-999999999999");
	private static final List<UUID> LINE_IDS = List.of(LINE_1, LINE_2, LINE_3);

	// ─── create ──────────────────────────────────────────────────────────────────

	@Nested
	@DisplayName("create")
	class Create {

		@Test
		@DisplayName("crée une commande PENDING avec une ligne PENDING par demande, rattachée à la commande")
		void createsPendingOrderWithOnePendingLinePerRequest() {
			CustomerOrder order = twoLineOrder();

			assertThat(order.getId()).isNotNull();
			assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
			assertThat(order.getPriority()).isEqualTo(Priority.HIGH);
			assertThat(order.isCompleteDeliveryRequired()).isTrue();
			assertThat(order.getCurrency()).isEqualTo("EUR");
			assertThat(order.getLineItems())
				.extracting(line -> line.getProductNr().getValue(), line -> line.getRequestedQty().getValue(),
					LineItem::getUnitPrice, LineItem::getStatus)
				.containsExactlyInAnyOrder(
					tuple("PROD01", 3, new BigDecimal("10.00"), PENDING),
					tuple("PROD02", 2, new BigDecimal("4.50"), PENDING));
			assertThat(order.getLineItems()).allSatisfy(line -> assertThat(line.getCustomerOrder()).isSameAs(order));
		}

		@Test
		@DisplayName("BUG CONNU : accepte une commande sans ligne")
		void acceptsOrderWithoutLines() {
			CustomerOrder order = CustomerOrder.create(Priority.NORMAL, false, "EUR", List.of());

			assertThat(order.getLineItems()).isEmpty();
		}
	}

	// ─── calculateTotal ──────────────────────────────────────────────────────────

	@Test
	@DisplayName("calculateTotal : somme de quantité × prix unitaire de chaque ligne")
	void calculateTotalSumsQuantityTimesUnitPrice() {
		// 3 × 10.00 + 2 × 4.50
		assertThat(twoLineOrder().calculateTotal()).isEqualByComparingTo("39.00");
	}

	// ─── lecture des lignes ──────────────────────────────────────────────────────

	@Nested
	@DisplayName("lecture des lignes")
	class LineReading {

		@Test
		@DisplayName("getLineItems renvoie une copie non modifiable")
		void getLineItemsReturnsUnmodifiableCopy() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			assertThatThrownBy(() -> order.getLineItems().clear()).isInstanceOf(UnsupportedOperationException.class);
			assertThat(order.getLineItems()).hasSize(1);
		}

		@Test
		@DisplayName("extractLineItemUuids renvoie l'identifiant de chaque ligne")
		void extractLineItemUuidsReturnsEveryLineId() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING, PENDING);

			assertThat(order.extractLineItemUuids()).containsExactlyInAnyOrderElementsOf(LINE_IDS);
		}
	}

	// ─── addLineItem ─────────────────────────────────────────────────────────────

	@Nested
	@DisplayName("addLineItem")
	class AddLineItem {

		@Test
		@DisplayName("ajoute une ligne PENDING rattachée à la commande")
		void addsPendingLineLinkedToOrder() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			LineItem added = order.addLineItem(new ProductNr("PROD09"), new Quantity(5), new BigDecimal("1.00"));

			assertThat(order.getLineItems()).hasSize(2).contains(added);
			assertThat(added.getStatus()).isEqualTo(PENDING);
			assertThat(added.getCustomerOrder()).isSameAs(order);
		}

		@Test
		@DisplayName("refuse l'ajout sur une commande annulée")
		void rejectsAdditionOnCancelledOrder() {
			CustomerOrder order = order(OrderStatus.CANCELLED, CANCELLED);

			assertRejectedWithoutSideEffect(order,
				() -> order.addLineItem(new ProductNr("PROD09"), new Quantity(5), BigDecimal.ONE),
				OrderCancellationException.class);
		}

		@ParameterizedTest(name = "{0}")
		@MethodSource("com.stock.management.domain.CustomerOrderTest#lineArgumentsWithOneNull")
		@DisplayName("BUG CONNU : accepte un argument null et crée une ligne invalide")
		void acceptsNullArgument(String nullArgument, ProductNr productNr, Quantity quantity, BigDecimal unitPrice) {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			order.addLineItem(productNr, quantity, unitPrice);

			assertThat(order.getLineItems()).hasSize(2);
		}
	}

	static Stream<Arguments> lineArgumentsWithOneNull() {
		ProductNr productNr = new ProductNr("PROD09");
		Quantity quantity = new Quantity(5);
		return Stream.of(
			arguments("productNr null", null, quantity, BigDecimal.ONE),
			arguments("requestedQty null", productNr, null, BigDecimal.ONE),
			arguments("unitPrice null", productNr, quantity, null));
	}

	// ─── extractEligibleLineIdsForCancellation ───────────────────────────────────

	@Nested
	@DisplayName("extractEligibleLineIdsForCancellation")
	class ExtractEligibleLineIds {

		@Test
		@DisplayName("renvoie les lignes demandées qui ne sont pas déjà annulées")
		void returnsRequestedLinesNotAlreadyCancelled() {
			CustomerOrder order = order(OrderStatus.PARTIALLY_ALLOCATED, CANCELLED, PENDING, FULLY_ALLOCATED);

			List<LineItemId> eligible = order.extractEligibleLineIdsForCancellation(LINE_IDS);

			assertThat(eligible).extracting(LineItemId::getValue).containsExactlyInAnyOrder(LINE_2, LINE_3);
		}

		@ParameterizedTest
		@NullAndEmptySource
		@DisplayName("renvoie une liste vide quand rien n'est demandé")
		void returnsEmptyListWhenNothingRequested(List<UUID> requested) {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			assertThat(order.extractEligibleLineIdsForCancellation(requested)).isEmpty();
		}

		@Test
		@DisplayName("refuse toute la demande si une ligne n'appartient pas à la commande")
		void rejectsLineNotBelongingToOrder() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING);

			assertThatThrownBy(() -> order.extractEligibleLineIdsForCancellation(List.of(LINE_1, UNKNOWN_LINE)))
				.isInstanceOf(LineItemNotFoundException.class);
		}

		@Test
		@DisplayName("BUG CONNU : sur une commande sans ligne, lève une NullPointerException au lieu de LineItemNotFoundException")
		void throwsNullPointerOnOrderWithoutLines() {
			CustomerOrder order = order(OrderStatus.PENDING);

			assertThatThrownBy(() -> order.extractEligibleLineIdsForCancellation(List.of(LINE_1)))
				.isInstanceOf(NullPointerException.class);
		}
	}

	// ─── cancelLines ─────────────────────────────────────────────────────────────

	@Nested
	@DisplayName("cancelLines")
	class CancelLines {

		@Test
		@DisplayName("annule uniquement les lignes ciblées")
		void cancelsOnlyTargetedLines() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING, PENDING);

			order.cancelLines(lineIds(LINE_1, LINE_2));

			assertThat(lineStatuses(order)).containsExactlyInAnyOrderEntriesOf(Map.of(
				LINE_1, CANCELLED,
				LINE_2, CANCELLED,
				LINE_3, PENDING));
		}

		@Test
		@DisplayName("tolère un identifiant en double")
		void toleratesDuplicateTarget() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING);

			order.cancelLines(lineIds(LINE_1, LINE_1));

			assertThat(lineStatuses(order)).containsExactlyInAnyOrderEntriesOf(Map.of(LINE_1, CANCELLED, LINE_2, PENDING));
		}

		@ParameterizedTest
		@NullAndEmptySource
		@DisplayName("refuse une liste de cibles nulle ou vide")
		void rejectsNullOrEmptyTargets(List<LineItemId> targets) {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			assertRejectedWithoutSideEffect(order, () -> order.cancelLines(targets), IllegalArgumentException.class);
		}

		@Test
		@DisplayName("BUG CONNU : une ligne inconnue lève une exception, mais les lignes connues sont déjà annulées")
		void unknownLineThrowsAfterCancellingKnownLines() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING);

			assertThatThrownBy(() -> order.cancelLines(lineIds(LINE_1, UNKNOWN_LINE)))
				.isInstanceOf(LineItemNotFoundException.class);

			assertThat(lineStatuses(order)).containsExactlyInAnyOrderEntriesOf(Map.of(LINE_1, CANCELLED, LINE_2, PENDING));
		}
	}

	// ─── evaluateAndModifyGlobalStatus ───────────────────────────────────────────

	@Nested
	@DisplayName("evaluateAndModifyGlobalStatus")
	class EvaluateGlobalStatus {

		@ParameterizedTest(name = "{0} + lignes {1} => {2}")
		@MethodSource("com.stock.management.domain.CustomerOrderTest#globalStatusScenarios")
		@DisplayName("déduit le statut de la commande du statut de ses lignes")
		void derivesOrderStatusFromLineStatuses(OrderStatus initial, List<LineItemStatus> lines, OrderStatus expected) {
			CustomerOrder order = order(initial, lines.toArray(LineItemStatus[]::new));

			order.evaluateAndModifyGlobalStatus();

			assertThat(order.getStatus()).isEqualTo(expected);
		}

		@Test
		@DisplayName("refuse une commande sans ligne")
		void rejectsOrderWithoutLines() {
			CustomerOrder order = order(OrderStatus.PENDING);

			assertRejectedWithoutSideEffect(order, order::evaluateAndModifyGlobalStatus, IllegalArgumentException.class);
		}

		@Test
		@DisplayName("refuse une ligne sans statut")
		void rejectsLineWithoutStatus() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING);
			lineById(order, LINE_2).changeLineStatus(null);

			assertThatThrownBy(order::evaluateAndModifyGlobalStatus).isInstanceOf(IllegalArgumentException.class);
			assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
		}
	}

	static Stream<Arguments> globalStatusScenarios() {
		return Stream.of(
			arguments(OrderStatus.PENDING, List.of(CANCELLED, CANCELLED, CANCELLED), OrderStatus.CANCELLED),
			arguments(OrderStatus.PENDING, List.of(FULLY_ALLOCATED, FULLY_ALLOCATED, FULLY_ALLOCATED), OrderStatus.FULLY_ALLOCATED),
			// Les lignes annulées ne comptent pas
			arguments(OrderStatus.PENDING, List.of(FULLY_ALLOCATED, FULLY_ALLOCATED, CANCELLED), OrderStatus.FULLY_ALLOCATED),
			arguments(OrderStatus.PENDING, List.of(PARTIALLY_ALLOCATED, PENDING, PENDING), OrderStatus.PARTIALLY_ALLOCATED),
			arguments(OrderStatus.PENDING, List.of(FULLY_ALLOCATED, NOT_ALLOCATED, PENDING), OrderStatus.PARTIALLY_ALLOCATED),
			arguments(OrderStatus.PENDING, List.of(CANCELLED, PARTIALLY_ALLOCATED, PENDING), OrderStatus.PARTIALLY_ALLOCATED),
			arguments(OrderStatus.PENDING, List.of(PENDING, PENDING, PENDING), OrderStatus.PENDING),
			// La seule ligne allouée vient d'être annulée : la commande redevient PENDING
			arguments(OrderStatus.PARTIALLY_ALLOCATED, List.of(CANCELLED, PENDING, NOT_ALLOCATED), OrderStatus.PENDING),
			// Livraison complète refusée : le statut ALLOCATION_FAILED est conservé
			arguments(OrderStatus.ALLOCATION_FAILED, List.of(NOT_ALLOCATED, NOT_ALLOCATED, NOT_ALLOCATED), OrderStatus.ALLOCATION_FAILED),
			arguments(OrderStatus.ALLOCATION_FAILED, List.of(CANCELLED, NOT_ALLOCATED, NOT_ALLOCATED), OrderStatus.ALLOCATION_FAILED));
	}

	// ─── cancel ──────────────────────────────────────────────────────────────────

	@Nested
	@DisplayName("cancel")
	class Cancel {

		@Test
		@DisplayName("annule la commande et toutes ses lignes, et trace l'annulation")
		void cancelsOrderAndAllLinesAndRecordsTraceability() {
			CustomerOrder order = order(OrderStatus.PARTIALLY_ALLOCATED, PENDING, CANCELLED, FULLY_ALLOCATED);
			Instant before = Instant.now();

			order.cancel(CancellationSource.CUSTOMER_APP, "changed mind", "user123");

			assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
			assertThat(lineStatuses(order).values()).containsOnly(CANCELLED);
			assertCancellationRecorded(order, CancellationSource.CUSTOMER_APP, "changed mind", "user123", before);
		}

		@ParameterizedTest
		@EnumSource(value = OrderStatus.class, names = {"CANCELLED", "FULLY_ALLOCATED"})
		@DisplayName("refuse une commande annulée ou entièrement allouée")
		void rejectsTerminalOrder(OrderStatus terminal) {
			CustomerOrder order = order(terminal, FULLY_ALLOCATED);

			assertRejectedWithoutSideEffect(order,
				() -> order.cancel(CancellationSource.CUSTOMER_APP, "reason", "user123"),
				IllegalStateException.class);
			assertThat(order.getCancelledAt()).isNull();
		}

		@Test
		@DisplayName("BUG CONNU : une commande sans ligne lève une NullPointerException au lieu d'IllegalStateException")
		void throwsNullPointerOnOrderWithoutLines() {
			CustomerOrder order = order(OrderStatus.PENDING);

			assertRejectedWithoutSideEffect(order,
				() -> order.cancel(CancellationSource.CUSTOMER_APP, "reason", "user123"),
				NullPointerException.class);
		}
	}

	// ─── recordCancellationIfFullyCancelled ──────────────────────────────────────

	@Nested
	@DisplayName("recordCancellationIfFullyCancelled")
	class RecordCancellationIfFullyCancelled {

		@Test
		@DisplayName("trace l'annulation d'une commande devenue CANCELLED par ses lignes")
		void recordsCancellationOfCancelledOrder() {
			CustomerOrder order = order(OrderStatus.CANCELLED, CANCELLED);
			Instant before = Instant.now();

			order.recordCancellationIfFullyCancelled(CancellationSource.CUSTOMER_SERVICE, "out of stock", "agent7");

			assertCancellationRecorded(order, CancellationSource.CUSTOMER_SERVICE, "out of stock", "agent7", before);
		}

		@Test
		@DisplayName("sans effet sur une commande qui n'est pas CANCELLED")
		void ignoresOrderNotCancelled() {
			CustomerOrder order = order(OrderStatus.PARTIALLY_ALLOCATED, CANCELLED, FULLY_ALLOCATED);

			order.recordCancellationIfFullyCancelled(CancellationSource.CUSTOMER_SERVICE, "out of stock", "agent7");

			assertThat(order.getCancelledAt()).isNull();
			assertThat(order.getCancellationSource()).isNull();
			assertThat(order.getCancelReason()).isNull();
			assertThat(order.getCancelBy()).isNull();
		}

		@Test
		@DisplayName("n'écrase jamais une annulation déjà tracée")
		void neverOverwritesRecordedCancellation() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);
			Instant before = Instant.now();
			order.cancel(CancellationSource.CUSTOMER_APP, "changed mind", "user123");

			order.recordCancellationIfFullyCancelled(CancellationSource.CUSTOMER_SERVICE, "other", "agent7");

			assertCancellationRecorded(order, CancellationSource.CUSTOMER_APP, "changed mind", "user123", before);
		}
	}

	// ─── validateCancellationEligibility / validateOrderStateForCancellation ─────

	@Nested
	@DisplayName("validations d'éligibilité à l'annulation")
	class CancellationEligibility {

		@ParameterizedTest
		@EnumSource(value = OrderStatus.class, names = {"CANCELLED", "FULLY_ALLOCATED"})
		@DisplayName("refusent une commande annulée ou entièrement allouée")
		void rejectTerminalOrder(OrderStatus terminal) {
			CustomerOrder order = order(terminal, PENDING);

			assertThatThrownBy(order::validateCancellationEligibility).isInstanceOf(OrderCancellationException.class);
			assertThatThrownBy(order::validateOrderStateForCancellation).isInstanceOf(OrderCancellationException.class);
		}

		@ParameterizedTest
		@EnumSource(value = OrderStatus.class, names = {"CANCELLED", "FULLY_ALLOCATED"}, mode = EnumSource.Mode.EXCLUDE)
		@DisplayName("acceptent tous les autres statuts")
		void acceptOtherStatuses(OrderStatus status) {
			CustomerOrder order = order(status, PENDING);

			assertThatCode(order::validateCancellationEligibility).doesNotThrowAnyException();
			assertThatCode(order::validateOrderStateForCancellation).doesNotThrowAnyException();
		}
	}

	// ─── updateOrderStatus ───────────────────────────────────────────────────────

	@Nested
	@DisplayName("updateOrderStatus")
	class UpdateOrderStatus {

		@Test
		@DisplayName("change le statut d'une commande en cours")
		void changesStatusOfOpenOrder() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			order.updateOrderStatus(OrderStatus.FULLY_ALLOCATED);

			assertThat(order.getStatus()).isEqualTo(OrderStatus.FULLY_ALLOCATED);
		}

		@ParameterizedTest
		@EnumSource(value = OrderStatus.class, names = {"CANCELLED", "FULLY_ALLOCATED"})
		@DisplayName("refuse de modifier une commande annulée ou entièrement allouée")
		void rejectsTerminalOrder(OrderStatus terminal) {
			CustomerOrder order = order(terminal, PENDING);

			assertRejectedWithoutSideEffect(order,
				() -> order.updateOrderStatus(OrderStatus.PENDING),
				InvalidOrderStateException.class);
		}

		@Test
		@DisplayName("BUG CONNU : accepte un statut null")
		void acceptsNullStatus() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			order.updateOrderStatus(null);

			assertThat(order.getStatus()).isNull();
		}
	}

	// ─── updateLineItemStatus ────────────────────────────────────────────────────

	@Nested
	@DisplayName("updateLineItemStatus")
	class UpdateLineItemStatus {

		@Test
		@DisplayName("change uniquement le statut des lignes demandées")
		void changesOnlyRequestedLines() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING, PENDING, PENDING);

			order.updateLineItemStatus(Map.of(LINE_1, FULLY_ALLOCATED, LINE_2, PARTIALLY_ALLOCATED));

			assertThat(lineStatuses(order)).containsExactlyInAnyOrderEntriesOf(Map.of(
				LINE_1, FULLY_ALLOCATED,
				LINE_2, PARTIALLY_ALLOCATED,
				LINE_3, PENDING));
		}

		@Test
		@DisplayName("sans effet avec une demande vide")
		void emptyRequestChangesNothing() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			order.updateLineItemStatus(Map.of());

			assertThat(lineStatuses(order)).containsExactlyEntriesOf(Map.of(LINE_1, PENDING));
		}

		@Test
		@DisplayName("refuse une demande nulle")
		void rejectsNullRequest() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);

			assertRejectedWithoutSideEffect(order, () -> order.updateLineItemStatus(null), NullPointerException.class);
		}

		@Test
		@DisplayName("BUG CONNU : une ligne inconnue lève une exception, mais les lignes traitées avant sont déjà modifiées")
		void unknownLineThrowsAfterChangingPreviousLines() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);
			// Ordre d'itération fixé : la ligne connue est traitée AVANT la ligne inconnue
			Map<UUID, LineItemStatus> updates = new LinkedHashMap<>();
			updates.put(LINE_1, FULLY_ALLOCATED);
			updates.put(UNKNOWN_LINE, FULLY_ALLOCATED);

			assertThatThrownBy(() -> order.updateLineItemStatus(updates)).isInstanceOf(IllegalArgumentException.class);

			assertThat(lineStatuses(order)).containsExactlyEntriesOf(Map.of(LINE_1, FULLY_ALLOCATED));
		}

		@Test
		@DisplayName("BUG CONNU : accepte un statut de ligne null")
		void acceptsNullLineStatus() {
			CustomerOrder order = order(OrderStatus.PENDING, PENDING);
			Map<UUID, LineItemStatus> updates = new HashMap<>();
			updates.put(LINE_1, null);

			order.updateLineItemStatus(updates);

			assertThat(lineById(order, LINE_1).getStatus()).isNull();
		}

		@Test
		@DisplayName("BUG CONNU : modifie les lignes d'une commande annulée")
		void changesLinesOfCancelledOrder() {
			CustomerOrder order = order(OrderStatus.CANCELLED, CANCELLED);

			order.updateLineItemStatus(Map.of(LINE_1, FULLY_ALLOCATED));

			assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
			assertThat(lineStatuses(order)).containsExactlyEntriesOf(Map.of(LINE_1, FULLY_ALLOCATED));
		}
	}

	// ─── Fixtures et assertions partagées ────────────────────────────────────────

	/** Commande créée par la factory métier : PROD01 (3 × 10.00) et PROD02 (2 × 4.50). */
	private static CustomerOrder twoLineOrder() {
		return CustomerOrder.create(Priority.HIGH, true, "EUR", List.of(
			new LineItemRequest("prod01", 3, new BigDecimal("10.00")),
			new LineItemRequest("PROD02", 2, new BigDecimal("4.50"))));
	}

	/** Commande au statut donné ; la ligne n°i reçoit LINE_i et le i-ème statut. */
	private static CustomerOrder order(OrderStatus status, LineItemStatus... lineStatuses) {
		List<LineItem> lines = new ArrayList<>();
		for (int i = 0; i < lineStatuses.length; i++) {
			lines.add(new LineItem(new LineItemId(LINE_IDS.get(i)), null, new ProductNr("PROD0" + (i + 1)),
				new Quantity(10), null, new BigDecimal("2.00"), lineStatuses[i]));
		}
		return new CustomerOrder(ORDER_ID, status, Priority.NORMAL, lines);
	}

	private static List<LineItemId> lineIds(UUID... uuids) {
		return Arrays.stream(uuids).map(LineItemId::new).toList();
	}

	/** Statut de chaque ligne, indexé par identifiant (jamais par position). */
	private static Map<UUID, LineItemStatus> lineStatuses(CustomerOrder order) {
		return order.getLineItems().stream()
			.collect(Collectors.toMap(line -> line.getId().getValue(), LineItem::getStatus));
	}

	private static LineItem lineById(CustomerOrder order, UUID lineId) {
		return order.getLineItems().stream()
			.filter(line -> line.getId().getValue().equals(lineId))
			.findFirst()
			.orElseThrow();
	}

	/** L'action échoue avec l'exception attendue et ne laisse aucune trace sur la commande ni sur ses lignes. */
	private static void assertRejectedWithoutSideEffect(CustomerOrder order, ThrowingCallable action,
														Class<? extends Throwable> expected) {
		OrderStatus statusBefore = order.getStatus();
		Map<UUID, LineItemStatus> linesBefore = lineStatuses(order);

		assertThatThrownBy(action).isInstanceOf(expected);

		assertThat(order.getStatus()).isEqualTo(statusBefore);
		assertThat(lineStatuses(order)).isEqualTo(linesBefore);
	}

	private static void assertCancellationRecorded(CustomerOrder order, CancellationSource source,
												   String reason, String cancelBy, Instant notBefore) {
		assertThat(order.getCancellationSource()).isEqualTo(source);
		assertThat(order.getCancelReason()).isEqualTo(reason);
		assertThat(order.getCancelBy()).isEqualTo(cancelBy);
		assertThat(order.getCancelledAt()).isNotNull().isAfterOrEqualTo(notBefore);
	}
}

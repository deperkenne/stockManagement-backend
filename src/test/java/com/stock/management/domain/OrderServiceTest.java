package com.stock.management.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stock.management.allocationLine.AllocationItemService;
import com.stock.management.kafka.producer.KafkaEventPublisher;
import com.stock.management.order.OrderOutboxRepository;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.OrderStatusHistoryRepository;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.CreateOrderResponse;
import com.stock.management.order.dto.LineItemRequest;
import com.stock.management.order.internal.OrderValidator;
import com.stock.management.sku.SkuService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * Tests unitaires purs : toutes les dépendances sont mockées, aucune base,
 * aucun Kafka, aucun contexte Spring chargé (pas de @SpringBootTest) → rapides et sans effet de bord.
 *
 * Hypothèses de reconstruction (à corriger si le vrai code diffère) :
 * - OrderId(String) est un value object simple
 * - CustomerOrder.create(...) est statique et déterministe
 * - orderMapper(order) et buildOrderReceivedEvent(order) sont des méthodes privées
 *   de OrderService, testées indirectement via leurs effets observables
 *   (contenu de la réponse / contenu de l'entrée outbox).
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock private AllocationItemService allocationItemService;
    @Mock private OrderRepository orderRepository;
    @Mock private OrderStatusHistoryRepository orderStatusHistoryRepository;
    @Mock private SkuService skuService;
    @Mock private OrderValidator orderValidator;
    @Mock private org.springframework.context.ApplicationEventPublisher eventPublisher;
    @Mock private KafkaEventPublisher kafkaEventPublisher;
    @Mock private ObjectMapper objectMapper;
    @Mock private OrderOutboxRepository outboxRepository;

    @InjectMocks
    private OrderService orderService;

    @Captor private ArgumentCaptor<OrderOutBox> outboxCaptor;
    @Captor private ArgumentCaptor<OrderStatusHistory> historyCaptor;

    private CreateOrderRequest validRequest;
    private CustomerOrder savedOrder;

    @BeforeEach
    void setUp() {
        LineItemRequest item01 = new LineItemRequest("SKU-1", 2, new BigDecimal(200.0));


		LineItem lineItem01 = new LineItem(
			new LineItemId(UUID.fromString("00000000-0000-0000-0000-0000000000a1")),
			null,
			new ProductNr("SKU-1"),
			new Quantity(2),
			Quantity.zero(),
			new BigDecimal("200.0"),
			LineItemStatus.PENDING
		);

        validRequest = new CreateOrderRequest(Priority.HIGH, false, "EUR", List.of(item01));
        savedOrder = new  CustomerOrder(new OrderId(UUID.fromString("00000000-0000-0000-0000-0000000000a1")),OrderStatus.PENDING,Priority.HIGH, List.of(lineItem01));
    }



    @Test
    @DisplayName("receiveOrder : chemin nominal — valide, persiste, historise, écrit l'outbox, ne touche pas Kafka directement")
    void receiveOrder_happyPath() throws JsonProcessingException {
        given(orderRepository.save(any(CustomerOrder.class))).willReturn(savedOrder);
        given(objectMapper.writeValueAsString(any())).willReturn("{}");

        CreateOrderResponse response = orderService.receiveOrder(validRequest);

        assertThat(response).isNotNull();
        assertThat(response.lineItemCount()).isEqualTo(1);

        verify(orderValidator).validateNoDuplicateProductNr(validRequest);
        verify(orderRepository).save(any(CustomerOrder.class));

        verify(orderStatusHistoryRepository).save(historyCaptor.capture());
        assertThat(historyCaptor.getValue().getReason()).isEqualTo("ORDER_RECEIVED");

        verify(outboxRepository).save(outboxCaptor.capture());
        OrderOutBox outbox = outboxCaptor.getValue();
        assertThat(outbox.getAggregateType()).isEqualTo("ORDER");
        assertThat(outbox.getEventType()).isEqualTo("OrderReceivedEvent");
        assertThat(outbox.getStatus()).isEqualTo(OrderOutBox.OutboxStatus.PENDING);
        assertThat(outbox.getRetryCount()).isZero();

        // le commentaire en tête de classe dit "sans Kafka" pour receiveOrder :
        // on vérifie qu'aucun envoi direct n'a lieu, tout passe par l'outbox
        verifyNoInteractions(kafkaEventPublisher, eventPublisher);
    }

    @Test
    @DisplayName("receiveOrder : requête invalide → arrêt immédiat, aucune persistance")
    void receiveOrder_invalidRequest_shortCircuits() {
        doThrow(new IllegalArgumentException("invalid")).when(orderValidator).validateNoDuplicateProductNr(any());

        assertThatThrownBy(() -> orderService.receiveOrder(validRequest))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(orderRepository, orderStatusHistoryRepository, outboxRepository);
    }

    @Test
    @DisplayName("receiveOrder : échec de sérialisation JSON → IllegalStateException, message clair")
    void receiveOrder_serializationFailure_wrapsException() throws JsonProcessingException {
        given(orderRepository.save(any(CustomerOrder.class))).willReturn(savedOrder);
        given(objectMapper.writeValueAsString(any()))
                .willThrow(new JsonProcessingException("boom") {});

        assertThatThrownBy(() -> orderService.receiveOrder(validRequest))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Erreur de sérialisation");

        // l'historique a déjà été écrit avant l'échec de sérialisation ; seul l'outbox est manqué
        verify(outboxRepository, never()).save(any());
    }

}

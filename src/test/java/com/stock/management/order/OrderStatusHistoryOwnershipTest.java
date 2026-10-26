package com.stock.management.order;

import com.stock.management.order.domain.OrderStatus;
import com.stock.management.order.domain.OrderStatusHistory;
import com.stock.management.order.dto.OrderStatusHistoryResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Un client ne reçoit que SON historique de statuts, jamais celui d'un autre client. */
@ExtendWith(MockitoExtension.class)
class OrderStatusHistoryOwnershipTest {

    private static final String ALICE = "kc-sub-alice";
    private static final String BOB = "kc-sub-bob";

    private final UUID aliceOrderId = UUID.randomUUID();
    private final UUID bobOrderId = UUID.randomUUID();

    @Mock
    private OrderStatusHistoryRepository orderStatusHistoryRepository;

    @InjectMocks
    private OrderStatusHistoryService orderStatusHistoryService;

    @Test
    @DisplayName("Alice ne reçoit que son historique, jamais celui de Bob")
    void findByCustomer_returnsOnlyOwnHistory() {
        given(orderStatusHistoryRepository.findByCustomerIdOrderByChangedAtAsc(ALICE)).willReturn(List.of(
                OrderStatusHistory.of(aliceOrderId, ALICE, null, OrderStatus.PENDING, "ORDER_RECEIVED"),
                OrderStatusHistory.of(aliceOrderId, ALICE, OrderStatus.PENDING, OrderStatus.FULLY_ALLOCATED, "ALLOCATED")
        ));

        List<OrderStatusHistoryResponse> result = orderStatusHistoryService.findByCustomer(ALICE);

        assertThat(result).hasSize(2)
                .extracting(OrderStatusHistoryResponse::orderId)
                .containsOnly(aliceOrderId.toString())
                .doesNotContain(bobOrderId.toString());
        verify(orderStatusHistoryRepository, never()).findAll();
        verify(orderStatusHistoryRepository, never()).findByCustomerIdOrderByChangedAtAsc(BOB);
    }

    @Test
    @DisplayName("Alice ne peut pas lire l'historique d'une commande de Bob (liste vide)")
    void findByOrderForCustomer_orderOfAnotherCustomer_isEmpty() {
        given(orderStatusHistoryRepository.findByOrderIdAndCustomerIdOrderByChangedAtAsc(bobOrderId, ALICE))
                .willReturn(List.of());

        List<OrderStatusHistoryResponse> result = orderStatusHistoryService.findByOrderForCustomer(bobOrderId, ALICE);

        assertThat(result).isEmpty();
        // Jamais de lecture par orderId seul, qui ignorerait le propriétaire
        verify(orderStatusHistoryRepository, never()).findByOrderIdOrderByChangedAtAsc(any());
    }
}

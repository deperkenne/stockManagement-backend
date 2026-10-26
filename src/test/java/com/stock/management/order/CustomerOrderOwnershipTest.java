package com.stock.management.order;

import com.stock.management.customer.Customer;
import com.stock.management.order.domain.CustomerOrder;
import com.stock.management.order.domain.OrderId;
import com.stock.management.order.domain.Priority;
import com.stock.management.order.dto.CreateOrderResponse;
import com.stock.management.order.dto.LineItemRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Un client ne reçoit que SES commandes, jamais celles d'un autre client. */
@ExtendWith(MockitoExtension.class)
class CustomerOrderOwnershipTest {

    private static final String ALICE = "kc-sub-alice";
    private static final String BOB = "kc-sub-bob";

    @Mock
    private OrderRepository orderRepository;

    @InjectMocks
    private OrderService orderService;

    private final CustomerOrder aliceOrder1 = order(ALICE);
    private final CustomerOrder aliceOrder2 = order(ALICE);
    private final CustomerOrder bobOrder = order(BOB);

    @Test
    @DisplayName("Alice ne reçoit que ses commandes, jamais celles de Bob")
    void findAllByCustomer_returnsOnlyOwnOrders() {
        given(orderRepository.findByCustomerIdOrderByReceivedAtDesc(ALICE))
                .willReturn(List.of(aliceOrder1, aliceOrder2));

        List<CreateOrderResponse> result = orderService.findAllByCustomer(ALICE);

        assertThat(result).allSatisfy(r -> assertThat(r.customerId()).isEqualTo(ALICE));
        assertThat(result).extracting(CreateOrderResponse::orderId)
                .containsExactlyInAnyOrder(aliceOrder1.getId().toString(), aliceOrder2.getId().toString())
                .doesNotContain(bobOrder.getId().toString());
        verify(orderRepository, never()).findAll();
    }

    @Test
    @DisplayName("Bob ne reçoit que ses commandes, jamais celles d'Alice")
    void findAllByCustomer_otherCustomer_returnsOnlyHisOrders() {
        given(orderRepository.findByCustomerIdOrderByReceivedAtDesc(BOB))
                .willReturn(List.of(bobOrder));

        List<CreateOrderResponse> result = orderService.findAllByCustomer(BOB);

        assertThat(result).allSatisfy(r -> assertThat(r.customerId()).isEqualTo(BOB));
        assertThat(result).extracting(CreateOrderResponse::orderId)
                .containsExactly(bobOrder.getId().toString())
                .doesNotContain(aliceOrder1.getId().toString(), aliceOrder2.getId().toString());
    }

    @Test
    @DisplayName("Alice ne peut pas lire le détail d'une commande de Bob (404)")
    void findByIdForCustomer_orderOfAnotherCustomer_isNotFound() {
        UUID bobOrderId = bobOrder.getId().getValue();
        given(orderRepository.findByIdAndCustomerIdWithLineItems(new OrderId(bobOrderId), ALICE))
                .willReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.findByIdForCustomer(bobOrderId, ALICE))
                .isInstanceOf(OrderNotFoundException.class);
    }

    private static CustomerOrder order(String customerId) {
        return CustomerOrder.create(Customer.register(customerId, null, null), Priority.NORMAL, false, "EUR",
                List.of(new LineItemRequest("PROD01", 1, new BigDecimal("10.00"))));
    }
}

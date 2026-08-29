package com.stock.management.domain;

import com.stock.management.order.OrderOutboxRepository;
import com.stock.management.order.OrderRepository;
import com.stock.management.order.OrderService;
import com.stock.management.order.OrderStatusHistoryRepository;
import com.stock.management.order.domain.*;
import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.LineItemRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@SpringBootTest
public class OrderServiceIT {
	@Autowired
	private OrderService orderService;

	@Autowired
	private OrderRepository orderRepository;

	@MockitoBean
	private OrderOutboxRepository outboxRepository;

	@Autowired
	private OrderStatusHistoryRepository orderStatusHistoryRepository;

	private CreateOrderRequest createOrderRequest;

	@Test
	void receiveOrder_shouldRollbackTransaction_whenSaveToOrderOutBoxErrorOccurs() {
		// GIVEN : Le composant de validation lève une exception
		doThrow(new IllegalArgumentException("invalid"))
			.when(outboxRepository)
			.save(any());

		LineItemRequest item01 = new LineItemRequest("SKU-1", 2, new BigDecimal(200.0));
		createOrderRequest = new CreateOrderRequest(Priority.HIGH, false, "EUR", List.of(item01));

		// WHEN : On tente de créer la commande
		assertThatThrownBy(() -> orderService.receiveOrder(createOrderRequest))
			.isInstanceOf(IllegalArgumentException.class);

		// THEN : On vérifie que la transaction a bien fait un ROLLBACK en base
		long totalOrdersInDb = orderRepository.count();
		long totalOrderStatusHist = orderStatusHistoryRepository.count();
		long totalOrderOutBox   = outboxRepository.count();

		assertThat(totalOrdersInDb).isZero();
		assertThat(totalOrderStatusHist).isZero();
		assertThat(totalOrderOutBox).isZero();
	}


}

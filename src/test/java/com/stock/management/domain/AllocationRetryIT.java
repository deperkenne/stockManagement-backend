package com.stock.management.domain;

import com.stock.management.allocation.AllocationReleasedEvent;
import com.stock.management.allocation.AllocationRetryService;
import com.stock.management.allocationLine.AllocationItem;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
public class AllocationRetryIT {
	@Autowired
	private ApplicationEventPublisher eventPublisher;

	@MockitoBean
	private AllocationRetryService allocationRetryService;

	@Autowired
	private TransactionTemplate transactionTemplate; // Permet de gérer les transactions explicitement dans le test

	@Test
	void shouldTriggerRetryOnlyAfterTransactionCommit() {
		// Given
		List items = List.of(mock(AllocationItem.class));
		AllocationReleasedEvent event = new AllocationReleasedEvent(items);

		// When : On simule une transaction qui réussit et commit
		transactionTemplate.execute(status -> {
			// L'événement est publié dans la transaction courante
			eventPublisher.publishEvent(event);

			// À ce stade (avant le commit), le listener ne doit PAS encore avoir été appelé
			verify(allocationRetryService, never()).retryPendingOrders(anyList());
			return null;
		});
		// <-- ICI : Le bloc transactionnel se termine et commit.
		// Le listener AFTER_COMMIT est donc déclenché juste après.

		// Then : On vérifie que le service de retry a bien été appelé après le commit
		verify(allocationRetryService, times(1)).retryPendingOrders(items);
	}
}

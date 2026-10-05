package com.stock.management.allocation;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.kafka.event.OrderReceivedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;
import java.util.UUID;
@Component
@Slf4j
@RequiredArgsConstructor
public class AllocationRetryListener {
	private final AllocationRetryService allocationRetryService;

	@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
	//@Transactional(propagation = Propagation.REQUIRES_NEW) // Ouvre une nouvelle transaction dédiée
	public void handleOrderCreated(AllocationReleasedEvent event) {

			log.info("\u001B[34m[CALL-RETRY-ALLOCATION] start retry process\u001B[0m");
			log.info("\u001B[34m[CALL-RETRY-ALLOCATION]  {}  start retry process\u001B[0m",event.allocationItems().size());
			allocationRetryService.retryPendingOrders(event.allocationItems());
	}
}

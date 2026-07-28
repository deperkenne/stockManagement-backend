package com.stock.management.allocationLine;

import com.stock.management.kafka.event.StockAllocatedEvent;
import com.stock.management.order.domain.AllocationStatus;
import com.stock.management.order.domain.ProductNr;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AllocationItemService {

	private final AllocationItemRepository allocationItemRepository;


	public List<AllocationItem> findAllByLineItemIdInAndSkuIdNotNull(List<UUID>items){
		return allocationItemRepository.findAllByLineItemIdInAndSkuIdNotNull(items);
	}


	public void changeAllocationItemStatus(List<UUID> items){

		allocationItemRepository.cancelAllByLineItemIds(items,AllocationItemStatus.CANCELLED,Instant.now().truncatedTo(ChronoUnit.MILLIS));
	}


	public void updateStatusToCancelledByOrderId(UUID orderId){
		allocationItemRepository.cancelAllItemsByOrderId(orderId, AllocationItemStatus.CANCELLED,Instant.now().truncatedTo(ChronoUnit.MILLIS));
	}


	@Transactional
	public void onStockAllocated(List<AllocationItem> lines) {
		if (lines == null || lines.isEmpty()) {
			log.warn("[ALLOCATION-ITEM-SERVICE] Received empty or null allocation items list. Nothing to persist.");
			return;
		}

		UUID orderId = lines.get(0).getOrderId();
		log.info("[ALLOCATION-ITEM-SERVICE] Processing {} allocation item(s) for Order: {}", lines.size(), orderId);

		// JPA/Hibernate appelle automatiquement @PrePersist avant d'insérer !
		List<AllocationItem> savedItems = allocationItemRepository.saveAll(lines);

		log.info("[ALLOCATION-ITEM-SERVICE] Successfully persisted {} allocation item(s) for Order: {}",
			savedItems.size(), orderId);
	}




	/**
	 * Persiste le résultat d'une allocation reçue de Kafka.
	 * Corrigé : status et allocatedAt n'étaient jamais renseignés (colonnes NOT NULL) — toute
	 * insertion échouait avec une violation de contrainte. skuId était construit avec
	 * UUID.fromString(line.getSku()) sans garde : line.getSku() est null pour NOT_ALLOCATED/
	 * WAITING_STOCK (cf. AllocationService), ce qui levait un NullPointerException à chaque
	 * allocation partielle ou échouée — précisément les cas que ce flux est censé traiter.
	 */
	@Transactional
	public void onStockAllocated1(StockAllocatedEvent event) {
		log.info("[ORDER-MODULE] Received StockAllocatedEvent for Order: {}", event.getOrderId());

		List<StockAllocatedEvent.AllocatedLine> lines = event.getAllocatedLines();
		if (lines == null || lines.isEmpty()) {
			return;
		}

		UUID orderId = UUID.fromString(event.getOrderId());
		Instant now = Instant.now();

		List<AllocationItem> items = lines.stream()
			.map(line -> AllocationItem.builder()
				.orderId(orderId)
				.lineItemId(UUID.fromString(line.getLineItemId()))
				.productNr(new ProductNr(line.getProductNr()))
				.quantity(line.getQuantityAllocated())
				.remainingQuantity(line.getRemainingQuantity())
				.skuId(line.getSku() != null ? UUID.fromString(line.getSku()) : null)
				.status(mapToDomainStatus(line.getAllocatedStatus()))
				.allocatedAt(now)
				.build())
			.toList();

		// saveAll() en un seul aller-retour au lieu d'un save() par ligne dans une boucle.
		allocationItemRepository.saveAll(items);

		log.info("[ORDER-MODULE] Order {} status synchronized with allocation results", event.getOrderId());
	}

	private AllocationItemStatus mapToDomainStatus(StockAllocatedEvent.AllocatedStatus eventStatus) {
		return switch (eventStatus) {
			case ALLOCATED -> AllocationItemStatus.ALLOCATED;
			case WAITING_STOCK -> AllocationItemStatus.WAITING_STOCK;
			case NOT_ALLOCATED -> AllocationItemStatus.NOT_ALLOCATED;
		};
	}

	// ─── CRUD (utilisé par AllocationItemController pour tester avec Postman) ─────

	/** CREATE — construit une nouvelle ligne d'allocation à partir du JSON reçu. allocatedAt est fixé au moment présent. */
	@Transactional
	public AllocationItem create(AllocationItemRequest request) {
		AllocationItem item = AllocationItem.builder()
			.orderId(request.orderId())
			.lineItemId(request.lineItemId())
			.productNr(new ProductNr(request.productNr()))
			.skuId(request.skuId())
			.quantity(request.quantity())
			.remainingQuantity(request.remainingQuantity())
			.status(request.status())
			.allocatedAt(Instant.now())
			.build();
		return allocationItemRepository.save(item);
	}

	/** READ (liste) — retourne toutes les lignes, y compris celles marquées "deleted" (soft delete historique). */
	public List<AllocationItem> findAll() {
		return allocationItemRepository.findAll();
	}

	/** Variante paginée — cette table grandit sans purge (historique des allocations) : à privilégier à grande échelle. */
	public Page<AllocationItem> findAll(Pageable pageable) {
		return allocationItemRepository.findAll(pageable);
	}

	/** READ (unitaire) — lève AllocationItemNotFoundException (404) si l'id n'existe pas. */
	public AllocationItem findById(Long id) {
		return allocationItemRepository.findById(id)
			.orElseThrow(() -> new AllocationItemNotFoundException("AllocationItem introuvable, id=" + id));
	}

	/** UPDATE — l'id vient toujours de l'URL, jamais du corps JSON : aucun risque de modifier la mauvaise ligne. */
	@Transactional
	public AllocationItem update(Long id, AllocationItemRequest request) {
		AllocationItem item = findById(id);
		item.setOrderId(request.orderId());
		item.setLineItemId(request.lineItemId());
		item.setProductNr(new ProductNr(request.productNr()));
		item.setSkuId(request.skuId());
		item.setQuantity(request.quantity());
		item.setRemainingQuantity(request.remainingQuantity());
		item.setStatus(request.status());
		return allocationItemRepository.save(item);
	}

	/** DELETE — suppression définitive. Aucune clé étrangère ne pointe vers "allocation_items" : jamais bloquée par une contrainte d'intégrité. */
	@Transactional
	public void delete(Long id) {
		if (!allocationItemRepository.existsById(id)) {
			throw new AllocationItemNotFoundException("AllocationItem introuvable, id=" + id);
		}
		allocationItemRepository.deleteById(id);
	}
}

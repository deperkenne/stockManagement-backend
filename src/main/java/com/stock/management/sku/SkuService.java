package com.stock.management.sku;

import com.stock.management.kafka.event.StockReleasedEvent;
import com.stock.management.order.domain.ProductNr;
import com.stock.management.order.domain.Quantity;
import com.stock.management.sku.domain.Sku;
import com.stock.management.sku.domain.SkuId;
import com.stock.management.sku.dto.CreateSkuRequest;
import com.stock.management.sku.dto.SkuResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SkuService {

    private final SkuRepository skuRepository;

    private record SkuLocationKey(String sku, String locationId) {}
	private final JdbcTemplate jdbcTemplate;


	private int[][] incrementAvailableStockInBatch(Map<SkuLocationKey, Integer> stockToRelease) {
		String sql = "UPDATE stock SET available_quantity = available_quantity + ? WHERE sku = ? AND location_id = ?";
		return jdbcTemplate.batchUpdate(sql,
			stockToRelease.entrySet(),
			stockToRelease.size(),
			(ps, entry) -> {
				ps.setInt(1, entry.getValue());
				ps.setString(2, entry.getKey().sku());
				ps.setString(3, entry.getKey().locationId());
			}
		);
	}

	@Transactional // 🔴 INDISPENSABLE pour garantir le ROLLBACK global en cas de violation d'intégrité
	public void releaseBulkStock(StockReleasedEvent event) {
		if (event == null) return;
		List<StockReleasedEvent.ReleasedLine> lines = event.getReleasedLines();
		if (lines == null || lines.isEmpty()) return;

		log.info("[STOCK-SERVICE] Batch stock release for order: {}", event.getOrderId());
		/**
		 * very well performence strategy
		 * grouping records  by  unique key , to perform only a single update per row
		 * and jdbTemplate.batchUpdate is the most effecient  approach  in spring for processing large  volumes of Data
		 *
		 */
		Map<SkuLocationKey, Integer> stockToRelease = lines.stream()
			.collect(Collectors.groupingBy(
				line -> new SkuLocationKey(line.getSku(), line.getLocationId()),
				Collectors.summingInt(StockReleasedEvent.ReleasedLine::getQuantityAllocated)
			));

		int[][] result = incrementAvailableStockInBatch(stockToRelease);

		boolean integrityViolation = Arrays.stream(result)
			.flatMapToInt(Arrays::stream)
			.anyMatch(r -> r == 0);
		if (integrityViolation) {
			log.error("[CRITICAL] Inventory integrity violation during batch release for order: {}", event.getOrderId());
			throw new IllegalStateException("Batch stock release failed: unmatched stock row.");
		}

		log.info("[STOCK-SERVICE] Released stock for {} distinct locations.", stockToRelease.size());
	}



    @Transactional
    public SkuResponse createSku(CreateSkuRequest request) {
        // Un même produit peut exister dans plusieurs emplacements.
        // Ce qui est interdit : le même produit dans le MÊME emplacement.
        if (skuRepository.existsByProductNrAndLocationCode(request.productNr(), request.locationCode())) {
            throw new DuplicateSkuException(
                    "SKU already exists for productNr=" + request.productNr()
                    + " at location=" + request.locationCode());
        }

        Sku sku = Sku.create(new ProductNr(request.productNr()),
                new Quantity(request.totalQuantity()), request.locationCode());
        sku = skuRepository.save(sku);

        log.info("[SKU] Created skuId={} productNr={} location={} qty={}",
                sku.getId(), sku.getProductNr().getValue(),
                sku.getLocation().getCode(), sku.getTotalQuantity().getValue());

        return toResponse(sku);
    }


    public SkuResponse findById(UUID id) {
        SkuId skuId = new SkuId(id);
        return skuRepository.findById(skuId)
                .map(this::toResponse)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + id));
    }

    public List<SkuResponse> findByProductNr(String productNr) {
        List<Sku> skus = skuRepository.findByProductNr(new ProductNr(productNr));
        if (skus.isEmpty()) {
            throw new SkuNotFoundException("No SKU found for productNr: " + productNr);
        }
        return skus.stream().map(this::toResponse).toList();
    }

    public void reserve(SkuId skuId, Quantity qty) {
        Sku sku = skuRepository.findByIdForUpdate(skuId)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + skuId));
        sku.reserve(qty);
        log.info("[SKU] Reserved qty={} skuId={} available={}",
                qty.getValue(), skuId, sku.getAvailableQuantity().getValue());
    }

	// reaprovisionement du stock des quantity liberera
    public void release(SkuId skuId, Quantity qty) {
        Sku sku = skuRepository.findByIdForUpdate(skuId)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + skuId));
        sku.release(qty); // actualisation du stock
        log.info("[SKU] Released qty={} skuId={} available={}",
                qty.getValue(), skuId, sku.getAvailableQuantity().getValue());
        // Note : le retry est notifié via StockReleasedEvent → handleStockReleased()
        // pour éviter un double-déclenchement (release() + handleStockReleased())
    }

    /**
     * Réapprovisionnement physique : nouveau stock reçu au warehouse.
     * Notifie immédiatement les commandes en attente de ce SKU.
     */
    public SkuResponse replenish(UUID id, int additionalQty) {
        SkuId skuId = new SkuId(id);
        Sku sku = skuRepository.findByIdForUpdate(skuId)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + id));

        sku.replenish(new Quantity(additionalQty));

        log.info("[SKU] Replenished qty={} skuId={} newAvailable={}",
                additionalQty, id, sku.getAvailableQuantity().getValue());

        // Déclenche immédiatement les retries en attente — aucun polling
        //allocationRetryService.notifyStockAvailable(sku.getProductNr().getValue());

        return toResponse(sku);
    }

    // ─── Private helpers ──────────────────────────────────────────────────────────

    private SkuResponse toResponse(Sku sku) {
        return new SkuResponse(
                sku.getId().toString(),
                sku.getProductNr().getValue(),
                sku.getTotalQuantity().getValue(),
                sku.getAvailableQuantity().getValue(),
                sku.getLocation().getCode(),
                sku.getLocation().isLocked(),
                sku.getLocation().getLockedReason()
        );
    }
}

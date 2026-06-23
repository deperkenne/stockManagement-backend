package com.stock.management.sku;

import com.stock.management.order.domain.ProductNr;
import com.stock.management.order.domain.Quantity;
import com.stock.management.sku.domain.Sku;
import com.stock.management.sku.domain.SkuId;
import com.stock.management.sku.dto.CreateSkuRequest;
import com.stock.management.sku.dto.SkuResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class SkuService {

    private final SkuRepository skuRepository;
    //private final AllocationRetryService allocationRetryService;

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

    @Transactional(readOnly = true)
    public SkuResponse findById(UUID id) {
        SkuId skuId = new SkuId(id);
        return skuRepository.findById(skuId)
                .map(this::toResponse)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + id));
    }

    @Transactional(readOnly = true)
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
        sku.release(qty);
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

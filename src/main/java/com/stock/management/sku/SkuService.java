package com.stock.management.sku;

import com.stock.management.allocationLine.AllocationItem;
import com.stock.management.kafka.event.OrderReceivedEvent;
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

	private final JdbcTemplate jdbcTemplate;


	public Map<String, List<Sku>> lockAndFetchAvailableStock(List<OrderReceivedEvent> events) {
		List<String> skuCodes = extractAndSortSkuCodes(events);

		if (skuCodes.isEmpty()) {
			return Map.of();
		}

		// recupere tous les sku et block ses ligne de sorte a ce que les autres transaction ne puisse pas acceder
		// attention cei vas echouer si la donner n'est pas persistente quand on vas redemarer le serveur et les consummer kafka vons
		// redemarer automatiquement
		// si on utilise H2 sa vas planter car la donnee ne serra plus memoire  au moment ou les consummer kafka vont rejouer cette methode
		// Verrouillage pessimiste en BDD
		List<Sku> allSkus = skuRepository.findAvailableSkusForAllocationWithLock(skuCodes);

		// Regroupement par ProductNr
		return allSkus.stream()
			.collect(Collectors.groupingBy(s -> s.getProductNr().getValue()));
	}



	private List<String> extractAndSortSkuCodes(List<OrderReceivedEvent> events) {
		return events.stream()
			.flatMap(e -> e.getLines().stream())
			.map(OrderReceivedEvent.OrderLine::getSku)
			.distinct()
			.sorted() // Tri alphabétique anti-deadlock
			.collect(Collectors.toList());
	}


	/**
	public List<Sku> findAvailableSkus(List<String>skuCodes){
		List<Sku> skus = skuRepository.findAvailableSkusForAllocationWithLock(skuCodes);
		 return skus;
	}

	 **/

	@Transactional // INDISPENSABLE pour garantir le ROLLBACK global en cas de violation d'intégrité
	public void releaseBulkStock(List<AllocationItem>allocationItems) {
		if (allocationItems == null || allocationItems.isEmpty()) {
			return;
		}

		log.info("[STOCK-SERVICE] Libération de stock en lot pour {} items d'allocation.", allocationItems.size());

		// 1. Conversion directe en Map (SKU -> Quantité à libérer)
		// Note : toMap plantera si deux items ont le même SKU.
		// Si des doublons de SKU sont possibles dans ta liste, utilise "Collectors.groupingBy" à la place.
		Map<UUID, Integer> stockToRelease = allocationItems.stream()
			.collect(Collectors.toMap(
				AllocationItem::getSkuId,
				AllocationItem::getQuantity
			));

		// 2. Exécution de la mise à jour en lot (Batch SQL)
		int[][] result = incrementAvailableStockInBatch(stockToRelease);

		// 3. Vérification de l'intégrité (on "aplatit" le tableau 2D en 1D avec flatMapToInt) le mettre dans une fonction separer
		boolean hasMissingSku = Arrays.stream(result)
			.flatMapToInt(Arrays::stream)
			.anyMatch(count -> count == 0);

		if (hasMissingSku) {
			log.error("[CRITICAL] Erreur d'intégrité : Impossible de trouver une ligne de stock pour l'un des SKU.");
			throw new IllegalStateException("La libération du stock en lot a échoué : SKU introuvable en base.");
		}

		log.info("[STOCK-SERVICE] Stock libéré avec succès pour {} SKUs.", stockToRelease.size());
	}

	private int[][] incrementAvailableStockInBatch(Map<UUID, Integer> stockToRelease) {
		// Requête simplifiée : on filtre uniquement par SKU désormais
		String sql = "UPDATE stock SET available_quantity = available_quantity + ? WHERE sku = ?";

		return jdbcTemplate.batchUpdate(
			sql,
			stockToRelease.entrySet(),
			stockToRelease.size(),
			(ps, entry) -> {
				ps.setInt(1, entry.getValue());
				ps.setString(2, entry.getKey().toString()); // Le SKU (la clé de ta Map)
			}
		);
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

    // READ (liste) — toutes les SKUs existantes, tous emplacements confondus.
    public List<SkuResponse> findAll() {
        return skuRepository.findAll().stream().map(this::toResponse).toList();
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

    @Transactional
    public void reserve(SkuId skuId, Quantity qty) {
        Sku sku = skuRepository.findByIdForUpdate(skuId)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + skuId));
        sku.reserve(qty);
        log.info("[SKU] Reserved qty={} skuId={} available={}",
                qty.getValue(), skuId, sku.getAvailableQuantity().getValue());
    }

	// reaprovisionement du stock des quantity liberera
    @Transactional
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
    @Transactional
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

    /**
     * UPDATE (PUT) — remplace productNr, totalQuantity et locationCode.
     * L'id (SkuId) reste toujours celui de l'URL : impossible de "voler" l'id d'un autre SKU via le body.
     */
    @Transactional
    public SkuResponse update(UUID id, CreateSkuRequest request) {
        SkuId skuId = new SkuId(id);
        Sku sku = skuRepository.findById(skuId)
                .orElseThrow(() -> new SkuNotFoundException("SKU not found: " + id));

        String newLocationCode = request.locationCode().trim().toUpperCase();
        boolean locationChanged = !newLocationCode.equals(sku.getLocation().getCode());
        if (locationChanged && skuRepository.existsByLocationCodeExcludingId(newLocationCode, skuId)) {
            throw new DuplicateSkuException("Un autre SKU utilise déjà l'emplacement: " + newLocationCode);
        }

        sku.updateDetails(new ProductNr(request.productNr()), new Quantity(request.totalQuantity()), request.locationCode());

        log.info("[SKU] Updated skuId={} productNr={} location={} totalQty={}",
                id, sku.getProductNr().getValue(), sku.getLocation().getCode(), sku.getTotalQuantity().getValue());

        return toResponse(sku);
    }

    /**
     * DELETE — supprime le SKU. cascade=ALL + orphanRemoval=true sur WarehouseLocation supprime
     * automatiquement l'emplacement associé. StockAllocation/AllocationItem stockent skuId en UUID
     * brut (pas de @JoinColumn JPA) : aucune contrainte de clé étrangère ne peut donc bloquer cette suppression.
     */
    @Transactional
    public void delete(UUID id) {
        SkuId skuId = new SkuId(id);
        if (!skuRepository.existsById(skuId)) {
            throw new SkuNotFoundException("SKU not found: " + id);
        }
        skuRepository.deleteById(skuId);
        log.info("[SKU] Deleted skuId={}", id);
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

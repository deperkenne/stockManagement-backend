package com.stock.management.support;

import com.stock.management.product.Product;
import com.stock.management.product.ProductRepository;
import com.stock.management.sku.WarehouseLocationRepository;
import com.stock.management.order.domain.ProductNr;
import com.stock.management.order.domain.Quantity;
import com.stock.management.sku.domain.Sku;
import com.stock.management.sku.domain.WarehouseLocation;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Données de référence pour les tests d'intégration.
 * Un Sku référence un WarehouseLocation existant (pas de cascade) et une commande lit le prix
 * dans le catalogue Product : les deux doivent donc exister en base avant le Sku / la commande.
 */
@Component
public class StockTestFixtures {

	public static final BigDecimal DEFAULT_UNIT_PRICE = new BigDecimal("10.00");

	private final WarehouseLocationRepository locationRepository;
	private final ProductRepository productRepository;

	public StockTestFixtures(WarehouseLocationRepository locationRepository, ProductRepository productRepository) {
		this.locationRepository = locationRepository;
		this.productRepository = productRepository;
	}

	/** Emplacement persisté, réutilisé s'il existe déjà (le code est unique). */
	public WarehouseLocation location(String code) {
		return locationRepository.findByCode(WarehouseLocation.normalizeCode(code))
			.orElseGet(() -> locationRepository.save(WarehouseLocation.create(code)));
	}

	/** Sku non encore sauvegardé, rattaché à un emplacement déjà persisté. */
	public Sku sku(String productNr, int quantity, String locationCode) {
		return Sku.create(new ProductNr(productNr), new Quantity(quantity), location(locationCode));
	}

	/** Crée les produits catalogue manquants, au prix DEFAULT_UNIT_PRICE. */
	public void products(String... productIds) {
		for (String id : productIds) {
			if (!productRepository.existsById(id)) {
				productRepository.save(new Product(id, id, DEFAULT_UNIT_PRICE));
			}
		}
	}
}

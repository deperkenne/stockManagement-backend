package com.stock.management.product;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ProductService {

		private final ProductRepository productRepository;

		public ProductService(ProductRepository productRepository) {
			this.productRepository = productRepository;
		}

		/**
		 * Récupère tous les produits du catalogue.
		 * Transaction en lecture seule pour optimiser les performances Hibernate (pas de dirty checking).
		 */
		@Transactional(readOnly = true)
		public List<ProductResponse> getAllProducts() {
			return productRepository.findAll()
				.stream()
				.map(ProductResponse::fromEntity)
				.toList();
		}

		/**
		 * Recherche un produit par son identifiant.
		 */
		@Transactional(readOnly = true)
		public ProductResponse getProductById(String id) {
			return productRepository.findById(id)
				.map(ProductResponse::fromEntity)
				.orElseThrow(() -> new ProductNotFoundException("Produit non trouvé avec l'identifiant : " + id));
		}

		/**
		 * Crée un nouveau produit catalogue.
		 */
		@Transactional
		public ProductResponse createProduct(CreateProductRequest request) {
			if (productRepository.existsById(request.id())) {
				throw new ProductAlreadyExistsException("Un produit avec l'ID '" + request.id() + "' existe déjà.");
			}

			Product product = new Product(request.id(), request.name());
			Product savedProduct = productRepository.save(product);

			return ProductResponse.fromEntity(savedProduct);
		}

		/**
		 * Supprime un produit du catalogue.
		 */
		@Transactional
		public void deleteProduct(String id) {
			if (!productRepository.existsById(id)) {
				throw new ProductNotFoundException("Impossible de supprimer : produit introuvable avec l'ID : " + id);
			}
			productRepository.deleteById(id);
		}

}

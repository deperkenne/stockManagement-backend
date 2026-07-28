package com.stock.management.product;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, String> {

	// Recherche par nom exacte ou partielle si besoin pour l'IHM
	boolean existsByName(String name);

	Optional<Product> findByNameContainingIgnoreCase(String keyword);
}

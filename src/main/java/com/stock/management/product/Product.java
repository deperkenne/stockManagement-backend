package com.stock.management.product;

import com.stock.management.sku.domain.Sku;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Entity
@Table(name = "product")
public class Product {
	@Id
	@Column(name = "id", length = 50)
	private String id; // Ex: "PRD-ROLLER-24V"

	@Column(name = "name", nullable = false, length = 150)
	private String name;

	// Factory method / Constructeur explicite (DDD)
	public Product(String id, String name) {
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("L'identifiant produit ne peut pas être vide.");
		}
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("Le nom du produit ne peut pas être vide.");
		}
		this.id = id;
		this.name = name;
	}

}

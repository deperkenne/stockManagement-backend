package com.stock.management.product;

import com.stock.management.sku.domain.Sku;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
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

	// Prix catalogue : seule source du prix d'une ligne de commande (jamais le corps de la requête client)
	@Column(name = "unit_price", nullable = false, precision = 19, scale = 2)
	private BigDecimal unitPrice;

	// Factory method / Constructeur explicite (DDD)
	public Product(String id, String name, BigDecimal unitPrice) {
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("L'identifiant produit ne peut pas être vide.");
		}
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("Le nom du produit ne peut pas être vide.");
		}
		if (unitPrice == null || unitPrice.signum() < 0) {
			throw new IllegalArgumentException("Le prix unitaire doit être renseigné et positif ou nul.");
		}
		this.id = id;
		this.name = name;
		this.unitPrice = unitPrice;
	}

}

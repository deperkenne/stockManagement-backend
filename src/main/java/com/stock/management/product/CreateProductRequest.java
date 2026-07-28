package com.stock.management.product;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateProductRequest(

	@NotBlank(message = "L'ID produit est obligatoire (ex: PRD-ROLLER-24V)")
	@Size(max = 50)
	String id,

	@NotBlank(message = "Le nom du produit est obligatoire")
	@Size(max = 150)
	String name
) {}

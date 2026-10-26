package com.stock.management.product;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateProductRequest(

	@NotBlank(message = "L'ID produit est obligatoire (ex: PRD-ROLLER-24V)")
	@Size(max = 50)
	String id,

	@NotBlank(message = "Le nom du produit est obligatoire")
	@Size(max = 150)
	String name,

	@NotNull(message = "Le prix unitaire est obligatoire")
	@DecimalMin(value = "0.00", message = "Le prix unitaire ne peut pas être négatif")
	@Digits(integer = 17, fraction = 2, message = "Le prix unitaire doit avoir au plus 2 décimales")
	BigDecimal unitPrice
) {}

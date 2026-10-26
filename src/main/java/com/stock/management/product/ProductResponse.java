package com.stock.management.product;

import java.math.BigDecimal;

public record ProductResponse(
	String id,
	String name,
	BigDecimal unitPrice
) {

	public static ProductResponse fromEntity(Product product) {
		return new ProductResponse(product.getId(), product.getName(), product.getUnitPrice());
	}

}

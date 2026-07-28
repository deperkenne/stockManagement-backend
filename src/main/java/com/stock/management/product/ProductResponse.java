package com.stock.management.product;

public record ProductResponse(
	String id,
	String name
) {

	public static ProductResponse fromEntity(Product product) {
		return new ProductResponse(product.getId(), product.getName());
	}

}

package com.stock.management.product;

public class ProductAlreadyExistsException extends RuntimeException {
	public ProductAlreadyExistsException(String message) {
		super(message);
	}
}

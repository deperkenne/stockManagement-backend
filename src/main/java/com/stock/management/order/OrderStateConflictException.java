package com.stock.management.order;

public class OrderStateConflictException extends RuntimeException {

	 public OrderStateConflictException(String message){
		 super(message);
	 }
}

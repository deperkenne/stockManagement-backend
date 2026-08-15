package com.stock.management.order;

public class InvalidOrderStateException extends RuntimeException {
  public InvalidOrderStateException(String message) {
    super(message);
  }
}

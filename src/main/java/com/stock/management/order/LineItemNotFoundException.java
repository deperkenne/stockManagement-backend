package com.stock.management.order;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class LineItemNotFoundException extends RuntimeException {
    public LineItemNotFoundException(String message) {
        super(message);
    }
}
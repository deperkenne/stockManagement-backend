package com.stock.management.allocationLine;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class AllocationItemNotFoundException extends RuntimeException {

    public AllocationItemNotFoundException(String message) {
        super(message);
    }
}
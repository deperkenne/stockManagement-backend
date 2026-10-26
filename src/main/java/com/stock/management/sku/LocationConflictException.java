package com.stock.management.sku;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/** Code d'emplacement déjà utilisé, ou emplacement occupé par un SKU (suppression refusée). */
@ResponseStatus(HttpStatus.CONFLICT)
public class LocationConflictException extends RuntimeException {

    public LocationConflictException(String message) {
        super(message);
    }
}

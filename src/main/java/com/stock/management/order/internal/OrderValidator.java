package com.stock.management.order.internal;

import com.stock.management.order.dto.CreateOrderRequest;
import com.stock.management.order.dto.LineItemRequest;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Component
public class OrderValidator {

    public void validate(CreateOrderRequest request) {
        validateNoDuplicateProductNr(request);
    }

    private void validateNoDuplicateProductNr(CreateOrderRequest request) {
        Set<String> seen = new HashSet<>();
        for (LineItemRequest li : request.lineItems()) {
            String normalized = li.productNr().trim().toUpperCase();
            if (!seen.add(normalized)) {
                throw new IllegalArgumentException(
                        "Duplicate productNr in order: " + li.productNr());
            }
        }
    }
}
package com.stock.management.order;

import com.stock.management.order.domain.OrderId;
import com.stock.management.order.dto.*;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    // CREATE — POST /api/orders : crée une commande avec ses lignes. Voir CreateOrderRequest pour le format JSON.
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateOrderResponse receiveOrder(@Valid @RequestBody CreateOrderRequest request) {
        //log.info("[REST] POST /api/orders externalOrderNr={}", request.externalOrderNr());
        return orderService.receiveOrder(request);
    }

    // READ (liste) — GET /api/orders : toutes les commandes (sans le détail des lignes, pour rester léger).
    @GetMapping
    public List<CreateOrderResponse> getAll() {
        return orderService.findAll();
    }

    // READ (unitaire) — GET /api/orders/{orderId} : une commande avec toutes ses lignes.
    @GetMapping("/{orderId}")
    public CreateOrderResponse getById(@PathVariable UUID orderId) {
        return orderService.findById(orderId);
    }

    /** Annule toute la commande et libère tout le stock alloué. */
    @PostMapping("/{orderId}/cancel")
    public CancelOrderResponse cancelOrder(@PathVariable UUID orderId,
                                            @Valid @RequestBody CancelOrderRequest request) {
        log.info("[REST] POST /api/orders/{}/cancel by={}", orderId, request.cancelledBy());
        return orderService.cancelOrder(new OrderId(orderId), request);
    }

    /** Annule une seule ligne et libère uniquement le stock alloué à cette ligne. */
    @PostMapping("/{orderId}/lines/{lineItemId}/cancel")
    public CancelOrderResponse cancelLineItem(@PathVariable UUID orderId,

                                               @Valid @RequestBody CancelOrderRequest request) {
        log.info("[REST] POST /api/orders/{}/lines/{}/cancel by={}", orderId, request.cancelledBy());
        return orderService.cancelLineItems(orderId,  request);
    }


    // CREATE — POST /api/orders/{orderId}/lines : ajoute une nouvelle ligne à une commande existante.
    // Corps JSON : { "productNr": "ABC123", "requestedQty": 5, "unitPrice": 19.90 }
    @PostMapping("/{orderId}/lines")
    @ResponseStatus(HttpStatus.CREATED)
    public CreateOrderResponse addLineItem(@PathVariable UUID orderId, @Valid @RequestBody LineItemRequest request) {
        log.info("[REST] POST /api/orders/{}/lines productNr={}", orderId, request.productNr());
        return orderService.addLineItem(orderId, request);
    }

}

package com.stock.management.order;

import com.stock.management.order.domain.OrderId;
import com.stock.management.order.dto.*;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import com.stock.management.shared.security.AuthenticatedCustomer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    // CREATE — POST /api/orders : crée une commande pour le client du token (claim "sub").
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
	@PreAuthorize("hasRole('CUSTOMER')")
    public CreateOrderResponse receiveOrder(@AuthenticationPrincipal Jwt jwt,
                                            @Valid @RequestBody CreateOrderRequest request) {
		log.info("received commande ..............");
        return orderService.receiveOrder(AuthenticatedCustomer.identityOf(jwt), request);
    }

    // READ (liste) — GET /api/v1/orders : uniquement les commandes du client connecté.
	@GetMapping("/client")
    public List<CreateOrderResponse> getAll(@AuthenticationPrincipal Jwt jwt) {
        return orderService.findAllByCustomer(AuthenticatedCustomer.idOf(jwt));
    }

	// READ (liste) — GET /api/orders : uniquement les commandes du client connecté.
	@GetMapping("/admin")
	public List<CreateOrderResponse>  getAllForAdmin() {
		return orderService.findAll();
	}

    // READ (unitaire) — GET /api/orders/{orderId} : 404 si la commande n'appartient pas au client connecté.
    @GetMapping("/{orderId}")
    public CreateOrderResponse getById(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID orderId) {
        return orderService.findByIdForCustomer(orderId, AuthenticatedCustomer.idOf(jwt));
    }


	@PostMapping("/{orderId}/cancel")
	public CancelOrderResponse cancelOrder(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID orderId,
										   @Valid @RequestBody CancelOrderRequest request) {
		orderService.findByIdForCustomer(orderId, AuthenticatedCustomer.idOf(jwt));
		log.info("[REST] POST /api/v1/orders/{}/cancel by={}", orderId, request.cancelledBy());
		return orderService.cancelOrder(new OrderId(orderId), request);
	}

	@PostMapping("/{orderId}/lines/{lineItemId}/cancel")
	public CancelOrderResponse cancelLineItem(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID orderId,
											  @PathVariable UUID lineItemId,
											  @Valid @RequestBody CancelOrderRequest request) {
		orderService.findByIdForCustomer(orderId, AuthenticatedCustomer.idOf(jwt));
		log.info("[REST] POST /api/v1/orders/{}/lines/{}/cancel by={}", orderId, lineItemId, request.cancelledBy());
		// La ligne du chemin fait foi.
		return orderService.cancelLineItems(orderId, new CancelOrderRequest(
			request.reason(), request.cancelledBy(), request.cancellationSource(), List.of(lineItemId)));
	}


	/** Annule toute la commande et libère tout le stock alloué.
    @PostMapping("/{orderId}/cancel")
    public CancelOrderResponse cancelOrder1(@PathVariable UUID orderId,
                                            @Valid @RequestBody CancelOrderRequest request) {
        log.info("[REST] POST /api/orders/{}/cancel by={}", orderId, request.cancelledBy());
        return orderService.cancelOrder(new OrderId(orderId), request);
    }

	 */

    /** Annule une seule ligne et libère uniquement le stock alloué à cette ligne.
    @PostMapping("/{orderId}/lines/{lineItemId}/cancel")
    public CancelOrderResponse cancelLineItem1(@PathVariable UUID orderId,

                                               @Valid @RequestBody CancelOrderRequest request) {
        log.info("[REST] POST /api/orders/{}/lines/{}/cancel by={}", orderId, request.cancelledBy());
        return orderService.cancelLineItems(orderId,  request);
    }
	 */

    // CREATE — POST /api/orders/{orderId}/lines : ajoute une nouvelle ligne à une commande existante.
    // Corps JSON : { "productNr": "ABC123", "requestedQty": 5, "unitPrice": 19.90 }
    @PostMapping("/{orderId}/lines")
    @ResponseStatus(HttpStatus.CREATED)
    public CreateOrderResponse addLineItem(@PathVariable UUID orderId, @Valid @RequestBody LineItemRequest request) {
        log.info("[REST] POST /api/orders/{}/lines productNr={}", orderId, request.productNr());
        return orderService.addLineItem(orderId, request);
    }

}

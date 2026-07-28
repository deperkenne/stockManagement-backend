package com.stock.management.allocationLine;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * CRUD complet sur AllocationItem — pensé pour être testé de bout en bout avec Postman.
 *
 * Base URL : /api/allocation-items
 *
 * Corps JSON pour POST et PUT :
 * {
 *   "orderId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "lineItemId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "productNr": "ABC123",
 *   "skuId": null,
 *   "quantity": 10,
 *   "remainingQuantity": 0,
 *   "status": "ALLOCATED"
 * }
 *
 * Gestion de l'id : l'id (Long) est un auto-incrément généré par la base de données.
 * Il n'apparaît JAMAIS dans le corps JSON envoyé — uniquement dans l'URL pour GET/PUT/DELETE.
 * Impossible donc d'avoir un conflit d'id en écrivant le mauvais id dans le body.
 */
@Slf4j
@RestController
@RequestMapping("/api/allocation-items")
@RequiredArgsConstructor
public class AllocationItemController {

    private final AllocationItemService allocationItemService;

    /** CREATE — crée une nouvelle ligne d'allocation. Réponse HTTP 201 + l'objet créé (avec son id généré). */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public AllocationItem create(@Valid @RequestBody AllocationItemRequest request) {
        log.info("[REST] POST /api/allocation-items orderId={}", request.orderId());
        return allocationItemService.create(request);
    }

    /** READ (liste) — retourne toutes les lignes d'allocation, y compris celles marquées "deleted" (soft delete historique). */
    @GetMapping
    public List<AllocationItem> getAll() {
        return allocationItemService.findAll();
    }

    /** READ (unitaire) — récupère une ligne par son id technique. Renvoie 404 si l'id n'existe pas. */
    @GetMapping("/{id}")
    public AllocationItem getById(@PathVariable Long id) {
        return allocationItemService.findById(id);
    }

    /** UPDATE — remplace toutes les valeurs de la ligne désignée par {id} dans l'URL. L'id du body est ignoré : il n'existe pas. */
    @PutMapping("/{id}")
    public AllocationItem update(@PathVariable Long id, @Valid @RequestBody AllocationItemRequest request) {
        log.info("[REST] PUT /api/allocation-items/{}", id);
        return allocationItemService.update(id, request);
    }

    /** DELETE — suppression définitive (hard delete). Table sans clé étrangère entrante : jamais bloquée par une contrainte d'intégrité. */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable Long id) {
        log.info("[REST] DELETE /api/allocation-items/{}", id);
        allocationItemService.delete(id);
    }
}
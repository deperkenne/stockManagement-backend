package com.stock.management.sku;

import com.stock.management.sku.dto.CreateSkuRequest;
import com.stock.management.sku.dto.ReplenishRequest;
import com.stock.management.sku.dto.SkuResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/skus")
@RequiredArgsConstructor
public class SkuController {

    private final SkuService skuService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SkuResponse createSku(@Valid @RequestBody CreateSkuRequest request) {
        log.info("[REST] POST /api/skus productNr={}", request.productNr());
        return skuService.createSku(request);
    }

    // READ (liste) — GET /api/skus : toutes les SKUs en base.
    @GetMapping
    public List<SkuResponse> getAll() {
        return skuService.findAll();
    }

    @GetMapping("/{id}")
    public SkuResponse getById(@PathVariable UUID id) {
        return skuService.findById(id);
    }

    // UPDATE — PUT /api/skus/{id} : remplace productNr, totalQuantity et locationCode.
    // Même corps JSON que la création (POST) :
    // { "productNr": "ABC123", "totalQuantity": 50, "locationCode": "A-01" }
    @PutMapping("/{id}")
    public SkuResponse update(@PathVariable UUID id, @Valid @RequestBody CreateSkuRequest request) {
        log.info("[REST] PUT /api/skus/{}", id);
        return skuService.update(id, request);
    }

    // DELETE — supprime définitivement le SKU et son emplacement (cascade automatique).
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) {
        log.info("[REST] DELETE /api/skus/{}", id);
        skuService.delete(id);
    }

    @GetMapping("/product-nr/{productNr}")
    public List<SkuResponse> getByProductNr(@PathVariable String productNr) {
        return skuService.findByProductNr(productNr);
    }

    @PostMapping("/{id}/replenish")
    public SkuResponse replenish(@PathVariable UUID id,
                                  @Valid @RequestBody ReplenishRequest request) {
        log.info("[REST] POST /api/skus/{}/replenish qty={}", id, request.quantity());
        return skuService.replenish(id, request.quantity());
    }
}
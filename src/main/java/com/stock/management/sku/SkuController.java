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

    @GetMapping("/{id}")
    public SkuResponse getById(@PathVariable UUID id) {
        return skuService.findById(id);
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
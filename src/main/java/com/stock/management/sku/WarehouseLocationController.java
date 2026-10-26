package com.stock.management.sku;

import com.stock.management.sku.dto.CreateLocationRequest;
import com.stock.management.sku.dto.LocationResponse;
import com.stock.management.sku.dto.LockLocationRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * Emplacements physiques de l'entrepôt.
 * Base URL : /api/v1/locations — corps JSON de POST/PUT : { "code": "A-01" }
 */
@RestController
@RequestMapping("/api/v1/locations")
@RequiredArgsConstructor
public class WarehouseLocationController {

    private final WarehouseLocationService locationService;

    /** CREATE — 201, ou 409 si le code existe déjà. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public LocationResponse create(@Valid @RequestBody CreateLocationRequest request) {
        return locationService.create(request);
    }




    @GetMapping("/{id}")
    public LocationResponse getById(@PathVariable UUID id) {
        return locationService.findById(id);
    }

    /** UPDATE — renomme l'emplacement (409 si le nouveau code existe déjà). */
    @PutMapping("/{id}")
    public LocationResponse rename(@PathVariable UUID id, @Valid @RequestBody CreateLocationRequest request) {
        return locationService.rename(id, request);
    }

    @PostMapping("/{id}/lock")
    public LocationResponse lock(@PathVariable UUID id, @Valid @RequestBody LockLocationRequest request) {
        return locationService.lock(id, request.reason());
    }

    @PostMapping("/{id}/unlock")
    public LocationResponse unlock(@PathVariable UUID id) {
        return locationService.unlock(id);
    }

    /** DELETE — 204, ou 409 si un SKU occupe encore l'emplacement. */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) {
        locationService.delete(id);
    }

	/** LIST — filtres optionnels : ?occupied=false&locked=false = emplacements libres. */
	@GetMapping
	public List<LocationResponse> findAll(@RequestParam(required = false) Boolean occupied,
										  @RequestParam(required = false) Boolean locked) {
		return locationService.findAll().stream()
			.filter(l -> occupied == null || l.occupied() == occupied)
			.filter(l -> locked == null || l.locked() == locked)
			.toList();
	}


}

package com.stock.management.order;

import com.stock.management.product.ProductAlreadyExistsException;
import com.stock.management.product.ProductNotFoundException;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.core.MethodParameter;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpHeaders;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Set;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Teste GlobalExceptionHandler via le vrai mécanisme de résolution Spring (MockMvc en mode
 * standalone + setControllerAdvice), pas par appel direct des méthodes : seule cette voie
 * exerce le même ExceptionHandlerMethodResolver que l'application réelle et aurait détecté
 * les doublons @ExceptionHandler(IllegalArgumentException.class) / @ExceptionHandler(DataIntegrityViolationException.class)
 * supprimés avant d'écrire ces tests (ils rendaient l'intégralité du controller advice inutilisable :
 * IllegalStateException: Ambiguous @ExceptionHandler dès la première exception interceptée).
 */
class GlobalExceptionHandlerTest {

	@FunctionalInterface
	private interface ThrowingAction {
		void run() throws Exception;
	}

	@RestController
	private static class ProbeController {
		private ThrowingAction action = () -> {};

		void setAction(ThrowingAction action) {
			this.action = action;
		}

		@GetMapping("/probe")
		public String probe() throws Exception {
			action.run();
			return "unreachable";
		}
	}

	private MockMvc mockMvc;
	private ProbeController controller;

	@BeforeEach
	void setUp() {
		controller = new ProbeController();
		mockMvc = MockMvcBuilders.standaloneSetup(controller)
			.setControllerAdvice(new GlobalExceptionHandler())
			.build();
	}

	@AfterEach
	void tearDown() {
		MDC.clear();
	}

	private ResultActions probe(ThrowingAction action) throws Exception {
		controller.setAction(action);
		return mockMvc.perform(get("/probe"));
	}

	// ── 1. IllegalArgumentException -> 400, style "build()" (code + timestamp + traceId) ──

	@Test
	void illegalArgument_mapsTo400WithInvalidArgumentCode() throws Exception {
		probe(() -> { throw new IllegalArgumentException("bad input"); })
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.detail").value("bad input"))
			.andExpect(jsonPath("$.code").value("INVALID_ARGUMENT"))
			.andExpect(jsonPath("$.timestamp").exists());
	}

	// ── 2. LineItemNotFoundException -> 404, style "manuel" (errorCode, pas de code/traceId) ──

	@Test
	void lineItemNotFound_mapsTo404WithErrorCode() throws Exception {
		probe(() -> { throw new LineItemNotFoundException("line item missing"); })
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.detail").value("line item missing"))
			.andExpect(jsonPath("$.title").value("Resource Not Found"))
			.andExpect(jsonPath("$.errorCode").value("LINE_ITEM_NOT_FOUND"))
			.andExpect(jsonPath("$.timestamp").exists())
			.andExpect(jsonPath("$.code").doesNotExist())
			.andExpect(jsonPath("$.traceId").doesNotExist());
	}

	// ── 3. OrderStateConflictException -> 409 ──

	@Test
	void orderStateConflict_mapsTo409WithErrorCode() throws Exception {
		probe(() -> { throw new OrderStateConflictException("already shipped"); })
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.detail").value("already shipped"))
			.andExpect(jsonPath("$.errorCode").value("ORDER_STATE_CONFLICT"));
	}

	// ── 4. IllegalStateException -> 409 ──

	@Test
	void illegalState_mapsTo409WithErrorCode() throws Exception {
		probe(() -> { throw new IllegalStateException("invalid transition"); })
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.detail").value("invalid transition"))
			.andExpect(jsonPath("$.errorCode").value("ILLEGAL_STATE_TRANSITION"));
	}

	// ── 5. NullPointerException -> 500, message interne masqué (pas de fuite de détail de bug) ──

	@Test
	void nullPointer_mapsTo500WithGenericMessage() throws Exception {
		probe(() -> { throw new NullPointerException("internal field was null"); })
			.andExpect(status().isInternalServerError())
			.andExpect(jsonPath("$.detail").value("An unexpected internal error occurred."))
			.andExpect(jsonPath("$.errorCode").value("NULL_POINTER_BUG"));
	}

	// ── 6. InvalidOrderStateException -> 409, message métier exposé tel quel ──

	@Test
	void invalidOrderState_mapsTo409AndExposesBusinessMessage() throws Exception {
		probe(() -> { throw new InvalidOrderStateException("Cannot cancel a shipped order"); })
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.detail").value("Cannot cancel a shipped order"))
			.andExpect(jsonPath("$.errorCode").value("INVALID_ORDER_STATE"));
	}

	// ── 7. DataAccessException générique (sans handler plus spécifique) -> 500, détail masqué ──

	@Test
	void genericDataAccessException_mapsTo500WithGenericMessage() throws Exception {
		probe(() -> { throw new InvalidDataAccessApiUsageException("raw SQL leaked here"); })
			.andExpect(status().isInternalServerError())
			.andExpect(jsonPath("$.detail").value("An unexpected infrastructure error occurred."))
			.andExpect(jsonPath("$.errorCode").value("DATABASE_ERROR"));
	}

	// ── 8. ProductNotFoundException -> 404 ──

	@Test
	void productNotFound_mapsTo404WithErrorCode() throws Exception {
		probe(() -> { throw new ProductNotFoundException("PROD-99 unknown"); })
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.detail").value("PROD-99 unknown"))
			.andExpect(jsonPath("$.errorCode").value("PRODUCT_NOT_FOUND"));
	}

	// ── 9. ProductAlreadyExistsException -> 409 ──

	@Test
	void productAlreadyExists_mapsTo409WithErrorCode() throws Exception {
		probe(() -> { throw new ProductAlreadyExistsException("PROD-01 already exists"); })
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.detail").value("PROD-01 already exists"))
			.andExpect(jsonPath("$.errorCode").value("PRODUCT_ALREADY_EXISTS"));
	}

	// ── 10. MethodArgumentNotValidException -> 400, passe par l'override handleExceptionInternal ──

	@Test
	void methodArgumentNotValid_mapsTo400WithFieldErrors() throws Exception {
		MethodParameter parameter = new MethodParameter(
			GlobalExceptionHandlerTest.class.getDeclaredMethod("dummyTarget", Object.class), 0);
		BeanPropertyBindingResult bindingResult = new BeanPropertyBindingResult(new Object(), "request");
		bindingResult.addError(new FieldError("request", "priority", "must not be null"));
		MethodArgumentNotValidException ex = new MethodArgumentNotValidException(parameter, bindingResult);

		probe(() -> { throw ex; })
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.detail").value("Validation failed"))
			.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
			.andExpect(jsonPath("$.errors[0].field").value("priority"))
			.andExpect(jsonPath("$.errors[0].message").value("must not be null"))
			.andExpect(jsonPath("$.timestamp").exists());
	}

	// used only via reflection above to build a real MethodParameter
	private void dummyTarget(Object value) {}

	// ── 11. ConstraintViolationException -> 400, violations réelles via le Validator Jakarta ──

	@Test
	void constraintViolation_mapsTo400WithFieldErrors() throws Exception {
		record Probe(@NotBlank String name) {}
		Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
		Set<ConstraintViolation<Probe>> violations = validator.validate(new Probe(""));
		ConstraintViolationException ex = new ConstraintViolationException(violations);

		probe(() -> { throw ex; })
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
			.andExpect(jsonPath("$.errors[0].field").value("name"))
			.andExpect(jsonPath("$.errors").isArray());
	}

	// ── 12/13. EntityNotFoundException et EmptyResultDataAccessException -> même handler 404 ──

	@Test
	void entityNotFound_mapsTo404WithGenericNotFoundCode() throws Exception {
		probe(() -> { throw new EntityNotFoundException("row missing"); })
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.detail").value("Resource not found"))
			.andExpect(jsonPath("$.code").value("NOT_FOUND"));
	}

	@Test
	void emptyResultDataAccess_mapsTo404WithGenericNotFoundCode() throws Exception {
		probe(() -> { throw new EmptyResultDataAccessException(1); })
			.andExpect(status().isNotFound())
			.andExpect(jsonPath("$.code").value("NOT_FOUND"));
	}

	// ── 14. DataIntegrityViolationException -> 409 ──

	@Test
	void dataIntegrityViolation_mapsTo409() throws Exception {
		probe(() -> { throw new DataIntegrityViolationException("duplicate key"); })
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.detail").value("The request conflicts with existing data or violates a constraint"))
			.andExpect(jsonPath("$.code").value("DATA_INTEGRITY"));
	}

	// ── 15. OptimisticLockingFailureException -> 409 ──

	@Test
	void optimisticLockingFailure_mapsTo409() throws Exception {
		probe(() -> { throw new OptimisticLockingFailureException("version mismatch"); })
			.andExpect(status().isConflict())
			.andExpect(jsonPath("$.code").value("OPTIMISTIC_LOCK"));
	}

	// ── 16. PessimisticLockingFailureException -> 503 + en-tête Retry-After ──

	@Test
	void pessimisticLockingFailure_mapsTo503WithRetryAfterHeader() throws Exception {
		probe(() -> { throw new PessimisticLockingFailureException("lock timeout"); })
			.andExpect(status().isServiceUnavailable())
			.andExpect(jsonPath("$.code").value("LOCK_TIMEOUT"))
			.andExpect(header().string(HttpHeaders.RETRY_AFTER, "1"));
	}

	// ── 17/18/19. Trois causes distinctes -> même handler "base indisponible" (503) ──

	@Test
	void queryTimeout_mapsTo503WithDbUnavailableCode() throws Exception {
		probe(() -> { throw new QueryTimeoutException("query too slow"); })
			.andExpect(status().isServiceUnavailable())
			.andExpect(jsonPath("$.code").value("DB_UNAVAILABLE"));
	}

	@Test
	void dataAccessResourceFailure_mapsTo503WithDbUnavailableCode() throws Exception {
		probe(() -> { throw new DataAccessResourceFailureException("connection refused"); })
			.andExpect(status().isServiceUnavailable())
			.andExpect(jsonPath("$.code").value("DB_UNAVAILABLE"));
	}

	@Test
	void cannotCreateTransaction_mapsTo503WithDbUnavailableCode() throws Exception {
		probe(() -> { throw new CannotCreateTransactionException("pool exhausted"); })
			.andExpect(status().isServiceUnavailable())
			.andExpect(jsonPath("$.code").value("DB_UNAVAILABLE"));
	}

	// ── 20. Filet de sécurité : toute autre exception -> 500 générique ──

	@Test
	void unexpectedException_mapsTo500WithGenericErrorCode() throws Exception {
		probe(() -> { throw new RuntimeException("totally unforeseen"); })
			.andExpect(status().isInternalServerError())
			.andExpect(jsonPath("$.detail").value("An unexpected error occurred"))
			.andExpect(jsonPath("$.code").value("INTERNAL_ERROR"));
	}

	// ── 21. enrich() : traceId propagé depuis le MDC quand présent ──

	@Test
	void traceId_isIncludedWhenPresentInMdc() throws Exception {
		MDC.put("traceId", "trace-abc-123");

		probe(() -> { throw new IllegalArgumentException("bad input"); })
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.traceId").value("trace-abc-123"));
	}

	// ── 22. enrich() : pas de clé traceId du tout quand le MDC est vide ──

	@Test
	void traceId_isAbsentWhenMdcEmpty() throws Exception {
		probe(() -> { throw new IllegalArgumentException("bad input"); })
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.traceId").doesNotExist());
	}
}
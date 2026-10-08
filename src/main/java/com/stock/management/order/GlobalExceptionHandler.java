package com.stock.management.order;

import com.stock.management.product.ProductAlreadyExistsException;
import com.stock.management.product.ProductNotFoundException;
import jakarta.persistence.EntityNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.*;
import org.springframework.http.*;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {
	// 2. Ressource Introuvable (HTTP 404)
	@ExceptionHandler(LineItemNotFoundException.class)
	public ProblemDetail handleLineItemNotFound(LineItemNotFoundException ex, HttpServletRequest request) {
		log.warn("[NOT_FOUND] Line item not found on path={} | Message={}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.NOT_FOUND,
			ex.getMessage()
		);
		problem.setTitle("Resource Not Found");
		problem.setProperty("errorCode", "LINE_ITEM_NOT_FOUND");
		problem.setProperty("timestamp", Instant.now());
		return problem;
	}

	// 3. Conflit d'état de commande métier (HTTP 409)
	@ExceptionHandler(OrderStateConflictException.class)
	public ProblemDetail handleOrderStateConflict(OrderStateConflictException ex, HttpServletRequest request) {
		log.warn("[DOMAIN_CONFLICT] Order state conflict on path={} | Message={}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.CONFLICT,
			ex.getMessage()
		);
		problem.setTitle("Status Conflict");
		problem.setProperty("errorCode", "ORDER_STATE_CONFLICT");
		problem.setProperty("timestamp", Instant.now());
		return problem;
	}

	// 4. Transition d'état illégale dans le Domaine (HTTP 409)
	@ExceptionHandler(IllegalStateException.class)
	public ProblemDetail handleIllegalState(IllegalStateException ex, HttpServletRequest request) {
		log.warn("[ILLEGAL_STATE] Illegal domain state transition on path={} | Message={}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.CONFLICT,
			ex.getMessage()
		);
		problem.setTitle("Illegal Domain State Transition");
		problem.setProperty("errorCode", "ILLEGAL_STATE_TRANSITION");
		problem.setProperty("timestamp", Instant.now());
		return problem;
	}

	// 5. Bug de Code / Crash Interne (HTTP 500)
	// log.error car cette erreur arrete tout le system
	@ExceptionHandler(NullPointerException.class)
	public ProblemDetail handleNullPointer(NullPointerException ex, HttpServletRequest request) {
		//  CRITIQUE : ex est passé en entier pour afficher la StackTrace et localiser le bug dans le code
		log.error("[SYSTEM_BUG] Unexpected NullPointerException on path={}", request.getRequestURI(), ex);

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.INTERNAL_SERVER_ERROR,
			"An unexpected internal error occurred."
		);
		problem.setTitle("Internal Server Error");
		problem.setProperty("errorCode", "NULL_POINTER_BUG");
		problem.setProperty("timestamp", Instant.now());
		return problem;
	}

	// 🟡 1. Conflit de statut / Invariant Domaine -> 409 CONFLICT
	@ExceptionHandler(InvalidOrderStateException.class)
	public ProblemDetail handleInvalidOrderState(InvalidOrderStateException ex, HttpServletRequest request) {
		// Log.warn sans stack trace conforme à ta politique
		log.warn("[DOMAIN REJECTION] Path: {} - Cause: {}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.CONFLICT,
			ex.getMessage() // ✅ Expose le VRAI message métier (ex: "Cannot cancel a shipped order")
		);

		problem.setTitle("Invalid Order State Transition");
		problem.setProperty("errorCode", "INVALID_ORDER_STATE");
		problem.setProperty("timestamp", Instant.now());

		return problem;
	}

	//  2. Tout le reste des erreurs BDD (Syntaxe, Panne DB, Verrous) -> 500 INTERNAL SERVER ERROR
	// une erreur 500 fait appel grace aux outils grafana a une arlerte
	// raison pour la quelle on utilise log.error  pour elle
	@ExceptionHandler(DataAccessException.class)
	public ProblemDetail handleGenericDatabaseException(DataAccessException ex) {
		log.error("[CRITICAL DB FAILURE]", ex); // On passe ex entier pour les pannes système (HTTP 500)

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.INTERNAL_SERVER_ERROR,
			"An unexpected infrastructure error occurred."
		);
		problem.setTitle("Database Failure");
		problem.setProperty("errorCode", "DATABASE_ERROR");
		return problem;
	}


	@ExceptionHandler(ProductNotFoundException.class)
	public ProblemDetail handleNotFound(ProductNotFoundException ex, HttpServletRequest request) {
		log.warn("[DOMAIN REJECTION] Path: {} - Cause: {}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.NOT_FOUND, //
			ex.getMessage()
		);

		problem.setTitle("Product Not Found"); //
		problem.setProperty("errorCode", "PRODUCT_NOT_FOUND"); //
		problem.setProperty("timestamp", Instant.now());

		return problem;
	}

	@ExceptionHandler(ProductAlreadyExistsException.class)
	public ProblemDetail handleAlreadyExists(ProductAlreadyExistsException ex, HttpServletRequest request) {
		log.warn("[DOMAIN REJECTION] Path: {} - Cause: {}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.CONFLICT, // 🎯 409 est approprié ici (conflit de création/doublon)
			ex.getMessage()
		);

		problem.setTitle("Product Already Exists"); // 🎯 Titre corrigé
		problem.setProperty("errorCode", "PRODUCT_ALREADY_EXISTS"); //
		problem.setProperty("timestamp", Instant.now());

		return problem;
	}



	/* ── 1. Exceptions métier : le statut et le code viennent de l'exception elle-même ──

	@ExceptionHandler(BusinessException.class)
	public ProblemDetail handleBusiness(BusinessException ex) {
		return build(ex.getStatus(), ex.getMessage(), ex.getErrorCode(), ex);
	}
    **/
	// ── 2. Validation ──

	/** @Valid sur un @RequestBody : on ajoute la liste des champs en erreur. */
	@Override
	protected ResponseEntity<Object> handleMethodArgumentNotValid(
		MethodArgumentNotValidException ex, HttpHeaders headers,
		HttpStatusCode status, WebRequest request) {

		ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Validation failed");
		pd.setProperty("code", "VALIDATION_ERROR");
		pd.setProperty("errors", ex.getBindingResult().getAllErrors().stream()
			.map(e -> Map.of(
				"field", e instanceof FieldError fe ? fe.getField() : e.getObjectName(),
				"message", String.valueOf(e.getDefaultMessage())))
			.toList());
		return handleExceptionInternal(ex, pd, headers, HttpStatus.BAD_REQUEST, request);
	}

	/**
	 * @Validated sur @PathVariable / @RequestParam ou validation au niveau service.
	 * Attention : Hibernate Validator peut aussi la lever au flush d'une entité JPA,
	 * ce qui est alors un bug serveur plutôt qu'une erreur client.
	 */
	@ExceptionHandler(ConstraintViolationException.class)
	public ProblemDetail handleConstraintViolation(ConstraintViolationException ex) {
		ProblemDetail pd = build(HttpStatus.BAD_REQUEST, "Validation failed", "VALIDATION_ERROR", ex);
		List<Map<String, String>> errors = ex.getConstraintViolations().stream()
			.map(v -> Map.of("field", v.getPropertyPath().toString(), "message", v.getMessage()))
			.toList();
		pd.setProperty("errors", errors);
		return pd;
	}

	/**
	 * Vos validateurs lèvent IllegalArgumentException (ex : doublon de productNr).
	 * Compromis : le message est exposé tel quel et une IAE levée par une bibliothèque serait
	 * aussi traduite en 400. Idéalement, remplacez-les par BusinessException.RuleViolation.
	 */
	@ExceptionHandler(IllegalArgumentException.class)
	public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
		return build(HttpStatus.BAD_REQUEST, ex.getMessage(), "INVALID_ARGUMENT", ex);
	}

	// ── 3. Persistance ──

	@ExceptionHandler({EntityNotFoundException.class, EmptyResultDataAccessException.class})
	public ProblemDetail handleNotFound(Exception ex) {
		return build(HttpStatus.NOT_FOUND, "Resource not found", "NOT_FOUND", ex);
	}

	/** Couvre aussi DuplicateKeyException. Message volontairement générique : pas de SQL ni de nom de contrainte. */
	@ExceptionHandler(DataIntegrityViolationException.class)
	public ProblemDetail handleDataIntegrity(DataIntegrityViolationException ex) {
		return build(HttpStatus.CONFLICT,
			"The request conflicts with existing data or violates a constraint", "DATA_INTEGRITY", ex);
	}

	/** Verrou optimiste (@Version) : quelqu'un a modifié la ressource entre la lecture et l'écriture. */
	@ExceptionHandler(OptimisticLockingFailureException.class)
	public ProblemDetail handleOptimisticLock(OptimisticLockingFailureException ex) {
		return build(HttpStatus.CONFLICT,
			"The resource was modified concurrently, reload it and retry", "OPTIMISTIC_LOCK", ex);
	}

	/** Verrou pessimiste : timeout de verrou, deadlock, sérialisation. Transitoire, donc 503 + Retry-After. */
	@ExceptionHandler(PessimisticLockingFailureException.class)
	public ResponseEntity<ProblemDetail> handleLockFailure(PessimisticLockingFailureException ex) {
		ProblemDetail pd = build(HttpStatus.SERVICE_UNAVAILABLE,
			"The resource is temporarily locked, retry shortly", "LOCK_TIMEOUT", ex);
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
			.header(HttpHeaders.RETRY_AFTER, "1")
			.body(pd);
	}

	/** Base indisponible ou requête trop lente : problème d'infrastructure, pas du client. */
	@ExceptionHandler({QueryTimeoutException.class,
		DataAccessResourceFailureException.class,
		CannotCreateTransactionException.class})
	public ProblemDetail handleDatabaseUnavailable(Exception ex) {
		return build(HttpStatus.SERVICE_UNAVAILABLE, "Service temporarily unavailable", "DB_UNAVAILABLE", ex);
	}

	// ── 4. Filet de sécurité : tout le reste = bug serveur, message générique ──

	@ExceptionHandler(Exception.class)
	public ProblemDetail handleUnexpected(Exception ex) {
		return build(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred", "INTERNAL_ERROR", ex);
	}

	// ── Infrastructure commune ──

	/** Toutes les réponses fabriquées par le parent passent ici : on y ajoute timestamp et traceId. */
	@Override
	protected ResponseEntity<Object> handleExceptionInternal(
		Exception ex, Object body, HttpHeaders headers,
		HttpStatusCode statusCode, WebRequest request) {

		if (statusCode.is5xxServerError()) {
			log.error("[{}] {}", statusCode.value(), ex.getMessage(), ex);
		} else {
			log.warn("[{}] {} ({})", statusCode.value(), ex.getMessage(), ex.getClass().getSimpleName());
		}
		if (body instanceof ProblemDetail pd) {
			enrich(pd);
		}
		return super.handleExceptionInternal(ex, body, headers, statusCode, request);
	}

	private ProblemDetail build(HttpStatus status, String detail, String code, Exception ex) {
		if (status.is5xxServerError()) {
			log.error("[{}] {}", code, detail, ex);
		} else {
			log.warn("[{}] {} ({})", code, detail, ex.getClass().getSimpleName());
		}
		ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
		pd.setProperty("code", code);
		return enrich(pd);
	}

	private ProblemDetail enrich(ProblemDetail pd) {
		pd.setProperty("timestamp", Instant.now());
		String traceId = MDC.get("traceId");
		if (traceId != null) {
			pd.setProperty("traceId", traceId);
		}
		return pd;
	}



}

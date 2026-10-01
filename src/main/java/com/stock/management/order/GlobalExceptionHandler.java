package com.stock.management.order;

import com.stock.management.product.ProductAlreadyExistsException;
import com.stock.management.product.ProductNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.Instant;
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {
	// 1. Validation/Arguments d'entrée incorrects (HTTP 400)
	@ExceptionHandler(IllegalArgumentException.class)
	public ProblemDetail handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
		log.warn("[BAD_REQUEST] Invalid input argument on path={} | Message={}", request.getRequestURI(), ex.getMessage());

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.BAD_REQUEST,
			ex.getMessage()
		);
		problem.setTitle("Invalid Input Argument");
		problem.setProperty("errorCode", "INVALID_ARGUMENT");
		problem.setProperty("timestamp", Instant.now());
		return problem;
	}

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

	// 🟡 1. Erreur fonctionnelle de donnée (Doublons, Foreign Key manquante, etc.) -> 409 CONFLICT
	@ExceptionHandler(DataIntegrityViolationException.class)
	public ProblemDetail handleDataIntegrityViolation(DataIntegrityViolationException ex) {
		log.warn("[DATA INTEGRITY VIOLATION] {}", ex.getMostSpecificCause().getMessage()); // le message court pour les erreurs métiers/client (HTTP 400/409).

		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
			HttpStatus.CONFLICT,
			"Data conflict: A resource with the same unique identifier already exists or violates database constraints."
		);
		problem.setTitle("Data Integrity Conflict");
		problem.setProperty("errorCode", "DUPLICATE_OR_CONSTRAINT_VIOLATION");
		return problem;
	}

	// 🔴 2. Tout le reste des erreurs BDD (Syntaxe, Panne DB, Verrous) -> 500 INTERNAL SERVER ERROR
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

}

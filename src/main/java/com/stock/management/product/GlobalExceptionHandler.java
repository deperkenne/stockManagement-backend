package com.stock.management.product;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Pour attraper les exceptions de votre service et renvoyer des réponses HTTP propres (404 Not Found, 409 Conflict) sans polluer le contrôleur
 * serra appeler quand une exeception de type @ExceptionHandler  serra raise
 * capture toustes les exception metier comme le catch et donne le messag avec le message d'erreur au client
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

	@ExceptionHandler(ProductNotFoundException.class)
	public ResponseEntity<String> handleNotFound(ProductNotFoundException ex) {
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
	}

	@ExceptionHandler(ProductAlreadyExistsException.class)
	public ResponseEntity<String> handleAlreadyExists(ProductAlreadyExistsException ex) {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
	}

}

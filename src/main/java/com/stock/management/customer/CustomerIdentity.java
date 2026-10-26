package com.stock.management.customer;

/**
 * Identité du client telle que lue dans le JWT Keycloak.
 *
 * @param id       claim "sub" (obligatoire, stable)
 * @param email    claim "email" (optionnel)
 * @param username claim "preferred_username" (optionnel)
 */
public record CustomerIdentity(String id, String email, String username) {
}

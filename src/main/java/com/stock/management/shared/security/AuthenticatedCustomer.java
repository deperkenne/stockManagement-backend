package com.stock.management.shared.security;

import com.stock.management.customer.CustomerIdentity;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.server.ResponseStatusException;

/**
 * Extrait l'identifiant du client connecté depuis le JWT Keycloak.
 * On utilise le claim "sub" : stable et unique, contrairement à preferred_username ou email.
 */
public final class AuthenticatedCustomer {

    private AuthenticatedCustomer() {
    }

    public static String idOf(Jwt jwt) {
        if (jwt == null || jwt.getSubject() == null || jwt.getSubject().isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "A valid bearer token is required");
        }
        return jwt.getSubject();
    }

    /** Identité complète (sub + email + username) pour créer le client à sa première commande. */
    public static CustomerIdentity identityOf(Jwt jwt) {
        return new CustomerIdentity(idOf(jwt), jwt.getClaimAsString("email"), jwt.getClaimAsString("preferred_username"));
    }
}

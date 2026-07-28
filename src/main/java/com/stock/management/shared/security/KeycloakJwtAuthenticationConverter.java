package com.stock.management.shared.security;

import org.springframework.core.convert.converter.Converter;
//import org.springframework.security.authentication.AbstractAuthenticationToken;
//import org.springframework.security.core.GrantedAuthority;
//import org.springframework.security.core.authority.SimpleGrantedAuthority;
//import org.springframework.security.oauth2.jwt.Jwt;
//import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extrait les rôles Keycloak depuis realm_access.roles et resource_access.{clientId}.roles,
 * les préfixe par ROLE_ pour être compatibles avec @PreAuthorize("hasRole('...')").
 */

/*
public class KeycloakJwtAuthenticationConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    private final String clientId;

    public KeycloakJwtAuthenticationConverter(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        Collection<GrantedAuthority> authorities = extractAuthorities(jwt);
        return new JwtAuthenticationToken(jwt, authorities, getPrincipalName(jwt));
    }

    private Collection<GrantedAuthority> extractAuthorities(Jwt jwt) {
        Set<GrantedAuthority> authorities = new HashSet<>();

        // Rôles globaux du realm : realm_access.roles
        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        if (realmAccess != null) {
            extractRoles(realmAccess.get("roles"), authorities);
        }

        // Rôles spécifiques au client : resource_access.{clientId}.roles
        if (clientId != null && !clientId.isBlank()) {
            Map<String, Object> resourceAccess = jwt.getClaimAsMap("resource_access");
            if (resourceAccess != null) {
                Object clientEntry = resourceAccess.get(clientId);
                if (clientEntry instanceof Map<?, ?> clientMap) {
                    extractRoles(clientMap.get("roles"), authorities);
                }
            }
        }

        return Collections.unmodifiableSet(authorities);
    }

    private void extractRoles(Object rawRoles, Set<GrantedAuthority> target) {
        if (rawRoles instanceof List<?> roles) {
            roles.stream()
                    .filter(String.class::isInstance)
                    .map(r -> new SimpleGrantedAuthority("ROLE_" + ((String) r).toUpperCase()))
                    .forEach(target::add);
        }
    }

    private String getPrincipalName(Jwt jwt) {
        if (jwt.hasClaim("preferred_username")) {
            return jwt.getClaimAsString("preferred_username");
        }
        return jwt.getSubject();
    }
}
*/

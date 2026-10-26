package com.stock.management.customer;

import com.stock.management.order.domain.CustomerOrder;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Client local, créé automatiquement (Just-In-Time) à sa première commande à partir du JWT Keycloak.
 */
@Entity
@Table(name = "customers")
@NoArgsConstructor(access = AccessLevel.PROTECTED) // requis par JPA
public class Customer {

	@Getter
	@Id
	private String id; // claim "sub" de Keycloak (ex: "f47ac10b-58cc...")

	@Getter
	private String email;

	@Getter
	private String username;

	@OneToMany(mappedBy = "customer", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<CustomerOrder> customerOrders = new ArrayList<>();

	public static Customer register(String id, String email, String username) {
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("customer id must not be blank");
		}
		Customer customer = new Customer();
		customer.id = id;
		customer.email = email;
		customer.username = username;
		return customer;
	}
}

package com.stock.management.customer;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CustomerRepository extends JpaRepository<Customer, String> {

	/**
	 * Crée le client s'il n'existe pas encore, sans erreur s'il existe déjà.
	 * ON CONFLICT DO NOTHING : si deux premières commandes du même client arrivent en même temps,
	 * la seconde n'échoue pas sur la clé primaire (un save() classique ferait échouer toute la transaction).
	 */
	@Modifying
	@Query(value = "INSERT INTO customers (id, email, username) VALUES (:id, :email, :username) " +
		"ON CONFLICT (id) DO NOTHING", nativeQuery = true)
	int insertIfAbsent(@Param("id") String id, @Param("email") String email, @Param("username") String username);
}

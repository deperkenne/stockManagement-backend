package com.stock.management.customer;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CustomerService {

	private final CustomerRepository customerRepository;

	/**
	 * Just-In-Time provisioning : renvoie le client du token, en le créant s'il n'existe pas encore.
	 * Rejoint la transaction de l'appelant : client et commande sont validés ensemble.
	 */
	@Transactional
	public Customer getOrCreate(CustomerIdentity identity) {
		return customerRepository.findById(identity.id())
			.orElseGet(() -> {
				customerRepository.insertIfAbsent(identity.id(), identity.email(), identity.username());
				return customerRepository.findById(identity.id()).orElseThrow();
			});
	}
}

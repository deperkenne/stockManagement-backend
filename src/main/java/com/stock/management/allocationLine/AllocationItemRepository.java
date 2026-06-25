package com.stock.management.allocationLine;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AllocationItemRepository extends JpaRepository<AllocationItem, Long> {

}


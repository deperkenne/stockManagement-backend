package com.stock.management.order;

import com.stock.management.order.domain.*;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<CustomerOrder, OrderId> {


    /**
     * Récupère uniquement le statut d'une commande par son ID.
     * Évite le chargement complet de l'entité et de ses relations (Performance).
     */
    @Query("SELECT o.status FROM CustomerOrder o WHERE o.id = :id")
    Optional<OrderStatus> findStatusById(@Param("id") OrderId id);

	// 1. Verrou sur la racine uniquement, sans jointure
	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("SELECT o FROM CustomerOrder o WHERE o.id = :id")
	Optional<CustomerOrder> findByIdForUpdate(@Param("id") OrderId id);

	/**
     * Mise à jour ciblée du statut d'une commande (utilisée par le flux d'allocation).
     * clearAutomatically/flushAutomatically évitent que le 1er niveau de cache renvoie
     * une entité obsolète si elle est relue dans la même transaction.
     */
	@Modifying
    @Query("UPDATE CustomerOrder o SET o.status = :status WHERE o.id = :orderId")
    int updateOrderStatus(@Param("orderId") OrderId orderId, @Param("status") OrderStatus status);

    /**
     * Met à jour le statut de plusieurs commandes en une seule requête (traitement batch).
     * Comparaison directe sur l'EmbeddedId (o.id IN :ids), pas sur son attribut interne.
     */
    @Modifying
    @Query("UPDATE CustomerOrder o SET o.status = :status WHERE o.id IN :ids")
    int updateStatusForIds(@Param("ids") List<OrderId> ids, @Param("status") OrderStatus status);

    /**
     * Met à jour le statut d'une seule ligne d'une commande (boucle d'allocation ligne par ligne).
     */
    @Modifying
    @Query("UPDATE LineItem l SET l.status = :lineStatus WHERE l.customerOrder.id = :orderId AND l.id = :lineId")
    int updateLineStatus(
            @Param("orderId") OrderId orderId,
            @Param("lineId") LineItemId lineId,
            @Param("lineStatus") LineItemStatus lineStatus
    );

    /**
     * Met à jour le statut de toutes les lignes actives d'une commande en une seule requête.
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE LineItem l SET l.status = :lineStatus WHERE l.customerOrder.id = :orderId AND l.status != :lineStatus")
    int updateAllLinesStatusIfChanged(@Param("orderId") OrderId orderId, @Param("lineStatus") LineItemStatus lineStatus);

    /**
     * Charge l'agrégat commande + ses lignes en une seule requête (évite le N+1 sur lineItems).
     * DISTINCT est indispensable : sans lui, une commande avec plusieurs lignes ferait remonter
     * plusieurs lignes SQL pour la même commande, et Optional/getSingleResult lèverait
     * NonUniqueResultException dès qu'une commande a 2 lignes ou plus.
     */
    @Query("SELECT DISTINCT o FROM CustomerOrder o LEFT JOIN FETCH o.lineItems WHERE o.id = :id")
    Optional<CustomerOrder> findByIdWithLineItems(@Param("id") OrderId id);

	@Query("SELECT DISTINCT o FROM CustomerOrder o LEFT JOIN FETCH o.lineItems WHERE o.id IN :ids")
	List<CustomerOrder> findAllByIdsWithLineItems(@Param("ids") List<OrderId> ids);

    /**
     * Charge la commande AVEC ses lignes, verrouillée en écriture (PESSIMISTIC_WRITE), en une seule requête.
     * Utilisée pour toute mutation de l'agrégat (annulation totale ou partielle) : un seul aller-retour
     * base de données au lieu de deux (lock puis lazy-load des lignes), et protège contre les mises à jour
     * concurrentes (ex: une allocation en cours sur la même commande). Un seul verrou sur une seule ligne
     * (celle de la commande) : aucun risque de deadlock, il n'y a pas d'ordre de verrouillage à respecter.
     * Pas de DISTINCT : PostgreSQL refuse "FOR UPDATE" avec DISTINCT, et Hibernate 6
     * dédoublonne déjà lui-même la commande ramenée par le JOIN FETCH.
     */
    //@Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM CustomerOrder o LEFT JOIN FETCH o.lineItems WHERE o.id = :id")
    Optional<CustomerOrder> findByIdWithLineItemsForUpdate(@Param("id") OrderId id);

	@Query("SELECT DISTINCT o FROM CustomerOrder o LEFT JOIN FETCH o.lineItems " +
		"WHERE o.id = :id AND o.status NOT IN :excludedStatuses")
	List<CustomerOrder> findModifiableOrderWithLineItems(
		@Param("productNrs") List<String> productNrs,
		@Param("excludedStatuses") List<OrderStatus> excludedStatuses);

	// Le filtre productNrs doit rester dans un EXISTS séparé : un WHERE sur l'alias
	// joint (li.productNr...) tronquerait aussi la collection lineItems fetchée,
	// ne laissant que les lignes du produit filtré au lieu de toutes les lignes de la commande.
	@Query("SELECT DISTINCT o FROM CustomerOrder o LEFT JOIN FETCH o.lineItems " +
		"WHERE o.status NOT IN :excludedStatuses " +
		"AND EXISTS (SELECT 1 FROM LineItem li2 WHERE li2.customerOrder = o AND li2.productNr.value IN :productNrs)")
	List<CustomerOrder> findModifiableOrderWithLineItem(
		@Param("productNrs") List<String> productNrs,
		@Param("excludedStatuses") List<OrderStatus> excludedStatuses);


	@Query("""
        SELECT DISTINCT o FROM CustomerOrder o
        JOIN FETCH o.lineItems li
        WHERE li.productNr.value IN :productNrs
        AND li.status = :status
        """)
	List<CustomerOrder> findOrdersHavingLineStatus(
		@Param("productNrs") List<String> productNrs,
		@Param("status") LineItemStatus status
	);

    /**
     * Commandes dans un statut donné dont au moins une ligne concerne un des productNr fournis.
     * Utilisée par AllocationRetryService pour rejouer les commandes ALLOCATION_FAILED après réapprovisionnement.
     * EXISTS garantit que TOUTES les lignes sont chargées (JOIN FETCH), pas seulement celles qui correspondent.
     */

	//@Lock(LockModeType.PESSIMISTIC_WRITE)
	//@Transactional(readOnly = true) // <-- Assure la transaction au niveau du Repository
	@Query("""
        SELECT DISTINCT o FROM CustomerOrder o
        JOIN o.lineItems item
        WHERE item.productNr.value IN :productNrs
        AND (
            o.status = 'ALLOCATION_FAILED'
            OR (o.status = 'PARTIALLY_ALLOCATED' AND item.status = 'NOT_ALLOCATED')
        )
    """)
    List<CustomerOrder> findByStatusAndLineProductNrs(
            @Param("productNrs") List<String> productNrs
    );
}

package com.stock.management.order.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Journal d'audit en append-only : une ligne par changement de statut d'une commande.
 * Alimente la couche analytique (temps passé par statut, SLA d'allocation, funnel de commande...).
 *
 * orderId est stocké en UUID brut (même pattern que AllocationItem) : cette table est un journal
 * de lecture pure, elle ne doit jamais être couplée par FK à customer_orders — elle doit survivre
 * même si la commande d'origine est un jour purgée.
 *
 * Volontairement pas de suivi équivalent au niveau LineItem : la granularité "commande" couvre le
 * besoin analytique actuel (durée par statut global). Le même pattern peut être dupliqué pour les
 * lignes si un besoin plus fin apparaît.
 */
@Entity
@Table(name = "order_status_history", indexes = {
        @Index(name = "idx_order_status_history_order_id", columnList = "order_id")
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderStatusHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    // null pour la toute première ligne (création de la commande : il n'y a pas de statut "avant")
    @Enumerated(EnumType.STRING)
    @Column(name = "previous_status", length = 30)
    private OrderStatus previousStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "new_status", nullable = false, length = 30)
    private OrderStatus newStatus;

    @Column(name = "changed_at", nullable = false)
    private Instant changedAt;

    // Courte étiquette d'origine du changement : "ORDER_RECEIVED", "CUSTOMER_APP", "ALLOCATION_FAILED"...
    @Column(name = "reason", length = 100)
    private String reason;

    public static OrderStatusHistory of(UUID orderId, OrderStatus previousStatus, OrderStatus newStatus, String reason) {
        OrderStatusHistory history = new OrderStatusHistory();
        history.orderId = orderId;
        history.previousStatus = previousStatus;
        history.newStatus = newStatus;
        history.changedAt = Instant.now();
        history.reason = reason;
        return history;
    }
}
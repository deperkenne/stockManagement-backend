# stock-management : Spring Boot, Java 21, Maven

## Commandes
- Tout : `mvn -q clean test`
- Une classe / une méthode : `mvn -q test -Dtest=Classe#methode`
- Intégration (*IT) : `mvn -q verify -Dit.test=ClasseIT`


## Règle métier : allocation des commandes

### Vocabulaire
- **Sku** : une ligne = un produit à un emplacement donné. Un même produit peut avoir
  plusieurs emplacements. `remainingQty` = quantité encore disponible à cet emplacement.
- **Order** : commande avec un booléen `completeDelivery`.
- **AllocationItem** : résultat de l'allocation d'une ligne de commande.
  Une ligne peut produire plusieurs AllocationItem (un par emplacement utilisé).

### Cas 1 : `completeDelivery = true` (livraison complète, tout ou rien)
1. Pour chaque produit de la commande, comparer la quantité demandée
   à la somme des `remainingQty` de ce produit dans Sku.
2. Si le stock est insuffisant pour au moins un produit :
  - la commande passe en `NOT_ALLOCATED`
  - toutes ses lignes passent en `NOT_ALLOCATED`
  - aucun stock n'est modifié, aucun AllocationItem n'est créé
3. Sinon, allouer toutes les lignes avec l'algorithme du cas 2.

### Cas 2 : `completeDelivery = false` (allocation partielle autorisée)
Pour chaque ligne de commande, parcourir les emplacements du produit dans Sku :
1. Prendre `min(quantité restante à allouer, remainingQty de l'emplacement)`.
2. Décrémenter `remainingQty` de l'emplacement dans Sku.
3. Créer un AllocationItem avec la quantité allouée.
4. Passer à l'emplacement suivant tant qu'il reste de la quantité à allouer.

**Exemple** : ligne PROD01, quantité = 30. Sku : emplacement A = 10, emplacement B = 40.
- A : on alloue 10, `remainingQty` de A passe à 0 → AllocationItem (10)
- B : on alloue 20, `remainingQty` de B passe à 20 → AllocationItem (20)

### Allocation incomplète d'une ligne
Si, après avoir parcouru tous les emplacements, la ligne n'est pas totalement allouée :
- reste = quantité demandée − somme des quantités allouées **pour cette ligne précise**
- ce reste est enregistré dans l'attribut `remainingQty` de AllocationItem
  (attention : ce n'est pas le même `remainingQty` que celui de Sku)

### Interdits
- Ne jamais rendre le `remainingQty` d'un Sku négatif.
- Ne jamais allouer partiellement une commande `completeDelivery = true`.


**Statuts d'une ligne de commande :**
| Statut | Signification |
|---|---|
| `PENDING` | Ligne nouvelle, pas encore traitée par l'allocation |
| `ALLOCATED` | Ligne totalement allouée (somme allouée = quantité demandée) |
| `NOT_ALLOCATED` | Ligne qui n'a reçu aucune allocation |
| `CANCELLED` | Ligne annulée, ignorée par l'allocation |

**Statuts d'une commande :**
| Statut | Condition |
|---|---|
| `ALLOCATED` | Toutes les lignes sont `ALLOCATED` |
| `PARTIAL_ALLOCATED` | `completeDelivery = false` et au moins une ligne n'est pas totalement allouée, même si les autres le sont |
| `NOT_ALLOCATED` | `completeDelivery = true` avec stock insuffisant (toutes ses lignes passent aussi en `NOT_ALLOCATED`) |

### Règles sur les statuts
- `completeDelivery = true` : seuls les résultats « tout alloué » ou « tout NOT_ALLOCATED » sont possibles. Jamais de `PARTIAL_ALLOCATED`.
- `completeDelivery = false` : la commande passe en `PARTIAL_ALLOCATED` dès qu'une ligne n'est pas totalement allouée.
- Les lignes `CANCELLED` ne sont pas allouées et ne comptent pas dans le calcul du statut de la commande.
- Seules les lignes `PENDING` sont traitées par l'allocation.

## Architecture

### Structure des packages
- `order/` : `CustomerOrder`, `OrderService`
  - `OrderService.receiveOrder()` enregistre la commande et écrit un événement dans la table **outbox** (même transaction)
- `allocation/` : `AllocationService`, `OutboxPublisher`, consumers Kafka
  - `OutboxPublisher` : lit l'outbox et publie sur Kafka (cycle toutes les 3 s)
  - Listener Kafka en **batch** : appelle `AllocationService.allocate()`

### Kafka
- Topic : `order.received-test03`, défini par la constante `ORDER_RECEIVED_TEST03`
- Toujours utiliser la constante, jamais la chaîne en dur
- Consumer : `consumer/OrderEventConsumerTest`

### Flux
`receiveOrder` → outbox → (OutboxPublisher, 3 s) → Kafka → listener batch → `allocate()`

### Pattern : Transactional Outbox
- La commande et l'événement outbox sont écrits dans **la même transaction** : jamais l'un sans l'autre.
- L'allocation est **asynchrone** : après `receiveOrder`, la commande est `PENDING`, elle n'est pas encore allouée.
- La livraison Kafka est *at-least-once* : `allocate()` doit être **idempotent** (une commande déjà traitée ne doit pas être réallouée ni décrémenter le stock deux fois).


## Conventions

- *Test =  *IT = @SpringBootTest avec vraie DB/Kafka
- Pas de @Transactional sur une classe de test si le code testé utilise
  REQUIRES_NEW ou un thread consumer : les données seraient invisibles
- Asynchrone : Awaitility, jamais Thread.sleep
- Ne jamais indexer une liste JPA par position : récupérer par ID


## Pièges connus
- OutboxPublisher publie toutes les 3 s, séquentiellement
- Après un changement d'annotation : `mvn clean` (bytecode périmé)

# Compact instructions
Conserver : fichiers modifiés, messages d'erreur exacts, décisions d'architecture.


## Annulation de commande

**Entrée** : `OrderService.cancelOrder(OrderId, CancelOrderRequest)`, `@Transactional` (transaction A).

### Flux
1. Fail-fast : `orderId`, `request`, `request.cancellationSource()` non null (`IllegalArgumentException`)
2. `findByIdWithLineItemsForUpdate` (verrou pessimiste) puis `validateCancellationEligibility()`
3. `order.cancel(source, reason, cancelledBy)` : commande et lignes → `CANCELLED`
4. Écriture `OrderStatusHistory` (ancien statut → `CANCELLED`, `CANCEL:<source>`)
5. **Uniquement si l'ancien statut = `PARTIALLY_ALLOCATED`** :
  - `releaseStockForPartialOrder` : `AllocationItem::cancel` sur les allocations actives
  - `skuService.releaseBulkStock(items)` : stock remis dans Sku (A)
  - `publishEvent(new AllocationReleasedEvent(items))`
6. Après commit de A : `@TransactionalEventListener(AFTER_COMMIT)` → `allocationRetryService.retryPendingOrders(items)` en **transaction B** (`REQUIRES_NEW`)

### Règles
- Commande sans allocation (`PENDING`, `NOT_ALLOCATED`) : statuts seulement, pas de stock, pas d'événement.
- `AllocationItem` muté via le domaine (`AllocationItem::cancel`), jamais par `UPDATE` SQL direct.
- Libérer uniquement les allocations actives avec `skuId` non null (le `remainingQty` d'un AllocationItem est un reste non alloué, pas du stock).
- Jamais de double libération (idempotence).
- Échec du retry (B) ne doit jamais annuler l'annulation (A).

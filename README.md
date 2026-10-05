# Démo visuelle Kafka 🚂

Application de démonstration pédagogique du fonctionnement de Kafka : producteur,
partitions, consumers (avec rebalance en direct), dead-letter topic, et manipulation
d'offsets — le tout visualisé en temps réel sous forme de "wagons" circulant entre
le producteur, les partitions du topic, les consumers et le DLT.

## Architecture

```
docker-compose.yml        → Zookeeper + Kafka (Confluent 7.6) + Kafka-UI
backend/                  → Application Spring Boot 3 / Java 21
  ├── config/             → WebSocket (STOMP/SockJS)
  ├── admin/              → Gestion des topics / partitions (AdminClient)
  ├── producer/           → Producteur Kafka planifié, configurable à chaud
  ├── consumer/           → Pool de "vrais" KafkaConsumer (même groupe → rebalance réel)
  ├── ws/                 → Diffusion des événements vers le front (STOMP)
  ├── controller/         → API REST de pilotage
  └── resources/static/   → Front HTML/CSS/JS unique (aucun framework)
```

Le front est une simple page servie par Spring Boot (`http://localhost:8080`), qui se
connecte en WebSocket (`/ws`, STOMP sur SockJS) pour recevoir tous les événements en
temps réel, et appelle l'API REST pour piloter le producteur, les consumers et le topic.

## Démarrage

### 1. Lancer l'infrastructure Kafka

```bash
docker compose up -d
```

- Kafka exposé sur `localhost:9092` (pour l'application Spring Boot)
- Kafka-UI disponible sur http://localhost:8081

### 2. Lancer l'application Spring Boot

```bash
cd backend
mvn spring-boot:run
```

L'application :
- crée automatiquement les topics `demo-topic` et `demo-topic-dlt` (3 partitions par défaut)
- démarre 2 consumers par défaut (groupe `demo-group`)

### 3. Ouvrir l'interface

http://localhost:8080

## Fonctionnalités de la démo

| Action | Effet |
|---|---|
| Démarrer/Arrêter le producteur | Envoie un message toutes les *N* ms vers `demo-topic` |
| Changer l'intervalle | Pris en compte immédiatement, sans redémarrage |
| Taux d'erreur (%) / "Générer une erreur" | Marque des messages comme invalides → routés vers le DLT par les consumers |
| Augmenter le nombre de partitions | Opération irréversible côté Kafka (impossible de redescendre) |
| ➕ / ➖ Consumers | Démarre/arrête un vrai `KafkaConsumer` du groupe `demo-group` → déclenche un **vrai rebalance Kafka**, visible dans Kafka-UI et dans l'animation |

## API REST principale

| Méthode | URL | Description |
|---|---|---|
| POST | `/api/producer/start` / `/stop` | Démarre/arrête le producteur |
| PUT | `/api/producer/interval` `{ "ms": 500 }` | Change l'intervalle d'envoi |
| PUT | `/api/producer/error-rate` `{ "percent": 10 }` | Taux d'erreur aléatoire |
| POST | `/api/producer/trigger-error` `{ "count": 1 }` | Force N messages en erreur |
| GET | `/api/topic` | Config actuelle du topic |
| PUT | `/api/topic/partitions` `{ "count": 6 }` | Augmente le nombre de partitions |
| GET | `/api/consumers` | Liste des consumers actifs et leurs partitions |
| PUT | `/api/consumers/count` `{ "count": 4 }` | Fixe le nombre de consumers actifs |
| POST | `/api/consumers/{id}/pause/{partition}` | Met en pause une partition pour un consumer |
| POST | `/api/consumers/{id}/resume/{partition}` | Reprend la consommation |
| POST | `/api/consumers/{id}/seek-beginning/{partition}` | Repositionne l'offset au début |
| POST | `/api/consumers/{id}/seek-end/{partition}` | Repositionne l'offset à la fin |
| POST | `/api/consumers/{id}/seek/{partition}` `{ "offset": 42 }` | Repositionne à un offset précis |

## Notes

- Tous les événements (production, consommation, rebalance, erreurs, DLT) sont diffusés
  sur le canal WebSocket `/topic/events` sous la forme `{ "type": ..., "payload": ... }`.
- Le front se reconnecte automatiquement en cas de coupure WebSocket.
- Les librairies front (SockJS, Stomp.js) sont chargées via CDN : prévoir un accès
  réseau depuis le navigateur, ou les héberger localement dans `static/js/` si la démo
  doit fonctionner hors ligne.

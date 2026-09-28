# Ticketing API

A ticket sales service for high-traffic on-sales, built with Rust (Actix-Web, Diesel), PostgreSQL, Redis, and Kafka. Users wait in a queue, are let in so that a fixed number shop at once, hold seats for a limited time, and confirm them by paying.

The service is being rebuilt; see [ROADMAP.md](ROADMAP.md) for the architecture and [STEPS.md](STEPS.md) for progress. Implemented so far: venues, events and their seat maps, registration and login, and joining the waiting room and checking your place in it. Admission, reservations and payment are not built yet.

## Components

| Component  | Role                                                                 |
|------------|----------------------------------------------------------------------|
| PostgreSQL | Durable record: users, venues, seats, events, bookings, payment inbox |
| Redis      | Tokens, seat map, active holds, hold expiry queue (AOF enabled)      |
| Kafka      | Ordered waiting room log, one topic per event                        |

## Running

Requirements: Rust (stable), Docker, `diesel_cli` with the PostgreSQL feature.

```bash
docker compose up -d
diesel migration run
cargo run
```

The server listens on `127.0.0.1:8080`. Configuration is read from the environment, with `.env` as a fallback; `DATABASE_URL` and `JWT_SECRET` are required.

| Variable                 | Default                   |
|--------------------------|---------------------------|
| `DATABASE_URL`           | required                  |
| `JWT_SECRET`             | required                  |
| `REDIS_URL`              | `redis://127.0.0.1:6379`  |
| `KAFKA_BROKERS`          | `localhost:9092`          |
| `DB_POOL_SIZE`           | `10`                      |
| `MAX_ACTIVE_TOKENS`      | `2000`                    |
| `TOKEN_TTL_SECS`         | `600`                     |
| `HOLD_TTL_SECS`          | `300`                     |
| `PAYMENT_EXTENSION_SECS` | `120`                     |
| `MAX_SEATS_PER_USER`     | `4`                       |
| `JWT_TTL_SECS`           | `3600`                    |

## Project Layout

```
src/main.rs          API server startup
src/lib.rs           Shared library used by the server and the workers in src/bin/
src/config.rs        Environment configuration
src/db.rs            PostgreSQL connection pool
src/cache.rs         Redis connection and per-event keys (seat bitmap, counters)
src/error.rs         ApiError: maps errors to JSON responses with status codes
src/auth.rs          Password hashing (Argon2id) and JWT issuing/validation
src/models.rs        Diesel models and enums
src/waiting.rs       WaitingQueue trait and its Kafka implementation
src/schema.rs        Diesel schema (generated)
src/actions/<area>.rs  Database operations, one file per area (users, venues, events,
                     bookings, payments); synchronous, take &mut PgConnection
src/api/mod.rs       Mounts every route group
src/api/<group>.rs   One file per route group (auth, venues, events, waiting_room,
                     reservations, payments), each exposing `configure`
migrations/          Database migrations
```

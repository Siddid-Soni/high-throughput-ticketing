# High-Throughput Ticketing

A ticket sales service for high-traffic on-sales, built with Rust (Actix-Web, Diesel), PostgreSQL, Redis and Kafka. Users wait in a queue and are let in so that a fixed number shop at once; they hold seats for a limited time and confirm them by paying.

[ROADMAP.md](ROADMAP.md) describes the architecture, [API_REFERENCE.md](API_REFERENCE.md) the endpoints, and [STEPS.md](STEPS.md) how it was built and tested. Not built yet: Stripe (a fake payment provider stands in).

## How a Sale Works

1. **Join.** Users join an event's waiting room. Before the sale opens they wait in a lobby, shuffled into a random order at the opening; afterwards the queue is first come, first served. The queue is a Kafka topic per event.
2. **Admission.** The admission worker lets the next user in whenever fewer than `min(MAX_ACTIVE_TOKENS, free seats)` are shopping. Admitted users see the seat map.
3. **Reserve.** A reservation holds seats in Redis for `HOLD_TTL_SECS`, all or none, and uses up the user's turn. It never touches PostgreSQL: this is the busiest step.
4. **Pay.** Paying extends the hold once and records the booking. The payment provider reports the outcome to an inbox table; a success claims the hold at once, so a payment that arrives in time is kept however busy the payment worker is. The worker then confirms the booking (seats sold) or refunds a payment that arrived after the hold ran out.
5. **Expire.** The cleanup worker releases holds whose time is up.

## Components

| Component        | Role                                                                    |
|------------------|-------------------------------------------------------------------------|
| API              | HTTP endpoints (`src/main.rs`)                                          |
| Admission worker | Opens and closes sales, admits users (`src/bin/admission_worker/`)      |
| Payment worker   | Processes payment events from the inbox (`src/bin/payment_worker.rs`)   |
| Cleanup worker   | Releases expired holds; rebuilds a lost seat map (`src/bin/cleanup_worker.rs`) |
| PostgreSQL       | Durable record: users, venues, seats, events, bookings, payment inbox   |
| Redis            | Tokens, seat map, holds, hold expiry queue (AOF enabled)                |
| Kafka            | Waiting room order, one single-partition topic per event                |

Every worker can run several copies: Kafka gives each event to one admission worker (the others stand by), and the payment and cleanup workers share the work.

## Running

Requirements: Rust (stable) and Docker.

Everything in containers:

```bash
docker compose --profile app up -d --build
```

Or only PostgreSQL, Redis and Kafka in containers, and the application with cargo:

```bash
docker compose up -d
cargo run                              # API; applies database migrations at startup
cargo run --bin admission_worker       # every scheduled and on-sale event (or pass an event id)
cargo run --bin payment_worker
cargo run --bin cleanup_worker
```

The API listens on `BIND_ADDR` (`127.0.0.1:8080`). Configuration comes from the environment, with `.env` as a fallback; `DATABASE_URL` and `JWT_SECRET` are required.

| Variable                 | Default                  | Meaning                                         |
|--------------------------|--------------------------|-------------------------------------------------|
| `DATABASE_URL`           | required                 |                                                 |
| `JWT_SECRET`             | required                 |                                                 |
| `BIND_ADDR`              | `127.0.0.1:8080`         | API address                                     |
| `REDIS_URL`              | `redis://127.0.0.1:6379` |                                                 |
| `KAFKA_BROKERS`          | `localhost:9092`         | `kafka:29092` from inside compose               |
| `DB_POOL_SIZE`           | `10`                     | Per process                                     |
| `REDIS_CONNECTIONS`      | `8`                      | Per API process; one becomes a bottleneck       |
| `MAX_ACTIVE_TOKENS`      | `2000`                   | Users shopping at once, per event               |
| `TOKEN_TTL_SECS`         | `600`                    | Time an admitted user has to reserve            |
| `HOLD_TTL_SECS`          | `300`                    | How long a reservation holds its seats          |
| `PAYMENT_EXTENSION_SECS` | `120`                    | Added once to a hold when payment starts        |
| `MAX_SEATS_PER_USER`     | `4`                      | Per event                                       |
| `JWT_TTL_SECS`           | `3600`                   |                                                 |
| `PAYMENT_PROVIDER`       | `fake`                   | Succeeds every payment                          |
| `FAKE_PAYMENT_DELAY_MS`  | `2000`                   | How long the fake provider takes to report      |

## Tests

```bash
docker compose up -d
cargo test                     # unit tests, plus Redis script and provider tests against the containers
load/reserve_race.sh 1000      # 1,000 admitted users reserve the same seat: exactly one wins
load/final.sh 10000            # 10,000 users, 100 seats, 70% pay; checks the outcome
load/capacity.sh 20000 1000 30 # 20,000 users polling every second for 30 s: latency and throughput
```

The load scripts run their own release builds of the API and workers and append their reports to [load/RESULTS.md](load/RESULTS.md). On one machine the ceiling is about 30,000-37,000 status polls per second, set by Redis; see the capacity summary there.

## Project Layout

```
src/main.rs            API server startup
src/lib.rs             Shared library used by the server and the workers
src/bin/               Admission, payment and cleanup workers
src/config.rs          Environment configuration
src/db.rs              PostgreSQL pool and embedded migrations
src/cache.rs           Redis connection, keys, and a wrapper per Lua script
src/waiting.rs         WaitingQueue trait and its Kafka implementation
src/payment/           PaymentProvider trait and the fake provider
src/auth.rs            Password hashing (Argon2id) and JWTs
src/error.rs           ApiError: maps errors to JSON responses with status codes
src/models.rs          Diesel models and enums
src/schema.rs          Diesel schema (generated)
src/actions/<area>.rs  Database operations, one file per area; synchronous
src/api/<group>.rs     One file per route group, each exposing `configure`
scripts/*.lua          Redis scripts: every multi-key change is one atomic script
migrations/            Database migrations
tests/                 Tests against Redis and PostgreSQL
load/                  Load tests and their results
```

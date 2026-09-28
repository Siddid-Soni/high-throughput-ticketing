# Roadmap: Waiting Room and Held Seats

Target architecture for high-traffic ticket sales: a waiting room that keeps a fixed number of users shopping at once, time-limited seat holds, and payment-confirmed bookings.

## Architecture

| Component          | Responsibility                                                           |
|--------------------|--------------------------------------------------------------------------|
| Gateway            | JWT authentication, routing (CDN in front later)                         |
| Waiting service    | Join the waiting room, report token status                               |
| Kafka              | Durable, ordered waiting room log (one single-partition topic per event) |
| Admission worker   | Reads the topic in order, keeps up to N tokens active (one per event)    |
| Booking service    | Reserve, pay, release; requires an active token                          |
| Cleanup worker     | Releases expired holds (horizontally scalable)                           |
| Payment worker     | Processes payment provider webhooks from the inbox (horizontally scalable) |
| Redis (AOF)        | Tokens, seating cache, active reservations, delay queue                  |
| PostgreSQL         | Durable record: venues, seats, events, event seats, bookings, inbox      |

Principle: Redis holds hot, short-lived state; PostgreSQL holds the durable record. Kafka carries the waiting room order. Asynchronous jobs (payment webhooks) use a PostgreSQL inbox table.

## Request Flow

1. **Join.** `POST /waiting-room/{event}/join` (authenticated). Idempotent per user via `user_token:{event}:{user}`. Before the sale opens, the token waits in a lobby (`LOBBY`); when the sale opens the lobby is shuffled into a random order and queued, so clicking first before opening gives no advantage. After opening, a join sets `token:{id}` to `QUEUED` and publishes the token id to the event topic `waiting.{event}`; the returned Kafka offset is the token's place in line (`seq = offset + 1`). Returns `202` with the token id. Closed events return `410`.
2. **Admission.** The admission worker consumes the topic in order (consumer group `admission-{event}`, offsets committed to Kafka) and keeps a fixed-size group of active tokens: it admits the next token only while the number of active tokens is below `min(MAX_ACTIVE_TOKENS, available seats)`. A slot frees when a token is used to reserve or expires unused, so admission follows the pace of the shoppers instead of a fixed rate; with no free seats the limit is 0 and admission waits until seats are released. The `admit.lua` script does the check and activation atomically; tokens that expired or are no longer `QUEUED` are skipped. Admitting sets the token `ACTIVE` with a TTL, adds it to `active_tokens:{event}`, and advances `admitted_upto:{event}`.
3. **Status.** `GET /waiting-room/{event}/status` (the caller's token, found through `user_token`; one Redis round trip) returns `LOBBY` (with the opening time), `QUEUED` (with `position = seq - admitted_upto`, an estimated wait from the recent admission rate, and `reason: all_seats_held` while every free seat is held or `sold_out` while every seat is paid for), `ACTIVE` (with remaining time), `CLOSED`, or `EXPIRED`. Sold out is not final: a cancellation reopens admission. Closing the sale is final.
   Only users whose token for the event is `ACTIVE` may read `GET /events/{id}/availability` (`403` otherwise); the layout is public and cacheable.
4. **Reserve.** `POST /reserve` runs a single Lua script that atomically: validates the active token, enforces the per-user seat limit, checks and sets the seat bits, writes `resv:{id}` with a TTL, adds the hold to `holds:expiry`, marks the token `USED`, and removes it from `active_tokens:{event}` (freeing a slot). Times come from `redis.call('TIME')`. Retrying with a used token returns the existing reservation. Postgres is not touched: this is the busiest step, so the hold lives only in Redis until payment starts.
5. **Pay.** `POST /pay/{resv}` requires a client payment idempotency key. The hold is extended once and a payment intent is created. The booking row is created here, as `PAYMENT_PENDING`, with the locked price and its `booking_seats`.
6. **Confirm.** The provider webhook is stored in `payment_inbox` and acknowledged with `200`. The payment worker claims the hold atomically (`confirm.lua` marks `resv:{id}` paid by this payment and removes it from `holds:expiry`); a retry of the same payment claims it again, so a crash before the commit is safe. On a claim it confirms in one transaction: `event_seats` to `SOLD` where still `AVAILABLE`, booking to `CONFIRMED`; then it deletes `resv:{id}`. If the hold is already gone (expired), the payment is refunded.
7. **Expire.** The cleanup worker claims due holds from `holds:expiry` with a Lua script. For each hold whose `resv:{id}` delete succeeds, it clears the seat bits and marks the booking `EXPIRED` if payment had started (otherwise there is no booking). Full batches are processed back-to-back.
8. **Release.** `DELETE /reserve/{id}` releases a hold immediately using the same path as expiry.

Payment and expiry race on the same hold: the payment claim removes it from `holds:expiry` and expiry deletes it, each atomically, so exactly one wins.

## Data Model

Tables: `users`, `venues`, `seats`, `events`, `event_seats`, `bookings`, `booking_seats`, `payment_inbox`.

Changes relative to the reference schema:

- `event_seats`: add `seat_index` (0..n-1 per event, bitfield offset); `UNIQUE (event_id, seat_id)` and `UNIQUE (event_id, seat_index)`; `status` as enum (`AVAILABLE`, `SOLD`, `BLOCKED`); `booking_id` set only when sold (enforced by a check); `version` used for optimistic confirmation.
- `bookings`: UUID `id` generated by the application (it is also the Redis reservation id); `status` as enum (`PENDING`, `PAYMENT_PENDING`, `CONFIRMED`, `EXPIRED`, `CANCELED`, `REFUNDED`); `payment_idempotency_key` and `payment_intent_id` (both unique).
- `booking_seats`: the seats each booking held, kept after expiry; used to rebuild Redis.
- `users`: surrogate `id`, unique `username`, `password_hash`.
- `payment_inbox`: `provider_event_id` (unique), `event_type`, `payload`, `processed_at`, `attempts`, `last_error`.
- Money is stored as integer cents (`price_cents`, `total_cents`).
- Indexes: `bookings (status, expires_at)`, `event_seats (event_id, status)`, and a partial index on unprocessed inbox rows.

Confirmation guard:

```sql
UPDATE event_seats
SET status = 'SOLD', booking_id = $1, version = version + 1
WHERE id = ANY($2) AND status = 'AVAILABLE';
-- affected rows must equal the seat count, otherwise roll back and refund
```

## Redis Keys

| Key                           | Type       | Purpose                                  |
|-------------------------------|------------|------------------------------------------|
| `token:{id}`                  | hash       | status, user, event, seq (Kafka offset + 1), published; TTL when active |
| `user_token:{event}:{user}`   | string     | Idempotent join                          |
| `admitted_upto:{event}`       | counter    | Last admitted sequence                   |
| `active_tokens:{event}`       | sorted set | Active tokens, score = token expiry in ms |
| `available:{event}`           | counter    | Free seats                               |
| `seat_count:{event}`, `sold:{event}` | counter | Total and paid seats (sold-out status) |
| `lobby:{event}`               | set        | Tokens that joined before the sale opened |
| `lobby_order:{event}`         | list       | Lobby after shuffling, published in order |
| `lobby_flushed:{event}`       | string     | Set once the lobby has been shuffled     |
| `admit_samples:{event}`       | list       | Recent `time:admitted_upto` samples (wait estimate) |
| `closed:{event}`              | string     | Set when the sale is closed              |
| `sale_opens_at:{event}`       | string     | Opening time, shown to lobby tokens      |
| `seats:{event}`               | bitfield   | Seating cache (1 = held or sold)         |
| `resv:{id}`                   | string     | Active reservation, JSON with `state` (`held`, `paying`, `paid`) and `paid_by` (TTL = hold + grace) |
| `holds:expiry`                | sorted set | Delay queue, score = expiry in ms        |
| `user_seats:{event}:{user}`   | counter    | Per-user seat limit                      |

## Stages

Each step is independently buildable and testable. The detailed checklist is in [STEPS.md](STEPS.md).

### Stage 0: Foundations
1. Read Kafka brokers, Redis URL, pool size, max active tokens, token TTL, hold TTL, and seat limit from the environment.
2. Add Redis (AOF enabled) and Kafka (KRaft, single node) to `docker-compose.yml`; add a Redis pool to application state.
3. ~~Baseline k6 load test against the legacy `/book`.~~ Dropped; the legacy code has been removed.

### Stage 1: Schema
4. Migration for the new tables, enums, constraints, and indexes.
5. Venue creation generating `seats` from `seat_map_config`.
6. Event creation generating `event_seats` with `seat_index` and price, and loading `seats:{event}` into Redis.
6a. Sale window: `sale_opens_at` on events.

### Stage 2: Authentication
7. JWT issuance and validation middleware; all user-facing endpoints derive the user from the token.

### Stage 3: Waiting Service
8. `WaitingQueue` abstraction with a Kafka implementation; one topic per event.
9. Join endpoint with idempotency, a pre-sale lobby, and sequence assignment.
10. Token status endpoint with position, estimated wait, and sold-out/closed states.
10a. Seat availability readable only with an `ACTIVE` token.

### Stage 4: Admission Worker
11. Consume the event topic in order and activate tokens.
12. Keep at most `min(MAX_ACTIVE_TOKENS, available seats)` tokens active; admit the next as slots free.
13. Skip expired tokens.
13a. Open the sale by shuffling the lobby into the queue.
13b. Record admission samples for the wait estimate.
13c. Sold-out status and closing the sale.

### Stage 5: Reservation
14. Reserve Lua script (token check, seat limit, seat bits, reservation, delay queue entry, token consumption).
15. `POST /reserve` (Redis only) with idempotent retry.
16. `DELETE /reserve/{id}`.

### Stage 6: Payment
17. `PaymentProvider` trait with a fake implementation; `POST /pay/{resv}` with idempotency key and one-time hold extension.
18. `payment_inbox` and payment worker with the retry-safe single-winner confirmation (claim, commit, finalize) and status-checked update.
19. Stripe test mode integration; refund path for payments that arrive after expiry.

### Stage 7: Expiry
20. Cleanup worker with the claim Lua script and batched Postgres updates.
21. Startup rebuild of `seats:{event}` from `event_seats` and the live `resv:{id}` holds.

### Stage 8: Consolidation
22. ~~Remove the legacy waitlist, `ConfirmationPending` flow, TTL queues, and RabbitMQ.~~ Done during Stage 1.
23. Split into binaries: `api`, `admission-worker`, `cleanup-worker`, `payment-worker`.
24. Final load test (10,000 users, 100 seats) with assertions: confirmed seats never exceed capacity, no seat sold twice, every expired hold released.

## Deferred

- Redis failover handling and Redis/Postgres reconciliation job.
- Bot mitigation (rate limiting, CAPTCHA, device fingerprinting).
- Running the waiting room at the CDN edge, and poll-interval hints that grow with queue position.
- Presence check that drops queued and admitted users who stop polling.
- Adjusting `MAX_ACTIVE_TOKENS` live from backend health.
- Notifications via an outbox.
- Metrics endpoint.
- CDN in front of `/events/{id}/layout`, which the API already marks as cacheable indefinitely. `/events/{id}/availability` is served directly from Redis and not cached.
- Elasticsearch (PostgreSQL full-text search until then).
- A shared partitioned topic (keyed by event) if the number of per-event topics grows too large.
- Swap to a managed queue behind the `WaitingQueue` abstraction.
- Take over maintenance of `kafkit-client` (fork it; the upstream repository is gone), or swap to `rdkafka` behind `WaitingQueue`.

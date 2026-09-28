# API Reference

All bodies are JSON unless noted. Errors are `{"message": "..."}` with the status codes listed per endpoint; `500` hides internal details (they are logged). Endpoints marked **auth** need `Authorization: Bearer <jwt>` from `POST /auth/login`, and return `401` without a valid one.

A purchase goes: join the waiting room, poll status until `ACTIVE`, read the seat map, reserve seats, pay. See [ROADMAP.md](ROADMAP.md) for how each step works.

## Authentication

### `POST /auth/register`
Body `{"username", "password"}`: username 3-50 characters of letters, digits, `_`, `-`, `.`; password 8-128 characters.
- `201 {"id", "username"}`
- `400` invalid username or password, `409` username taken

### `POST /auth/login`
Body `{"username", "password"}`.
- `200 {"token", "expires_at"}`: the JWT, valid for `JWT_TTL_SECS`
- `401` wrong username or password

### `GET /auth/me` (auth)
- `200 {"id"}`: the caller's user id

## Venues and Events

These are unauthenticated (there is no admin role yet).

### `POST /venue`
Body `{"name", "location", "seat_map_config": {"sections": [{"name", "rows", "seats_per_row", "type"?}]}}`. `type` defaults to `standard`; at most 100,000 seats; section names are unique. Creates one seat per row and position.
- `201` the venue (`id`, `name`, `location`, `seat_map_config`, `created_at`) plus `seat_count`
- `400` invalid layout

### `POST /events`
Body `{"venue_id", "name", "start_time", "sale_opens_at", "prices": {"<section>": cents}}`. Both times in the future, `sale_opens_at` before `start_time`, a non-negative price for every section of the venue. Creates the event's seats (numbered `seat_index` 0..n-1), its seat map in Redis and its Kafka topic.
- `201` the event (`id`, `venue_id`, `name`, `start_time`, `sale_opens_at`, `status`, `created_at`) plus `seat_count`
- `400` invalid times or prices, `404` venue not found, `409` event name taken

The status starts `SCHEDULED`, becomes `ON_SALE` at `sale_opens_at` and `CLOSED` at `start_time` (both done by the admission worker).

### `GET /events/{id}/layout`
Public and immutable, so browsers and CDNs cache it for a year.
- `200 {"event_id", "seat_count", "seats": [{"seat_index", "section", "row_number", "seat_number", "type", "price_cents"}]}`: `seats[i]` is bit `i` of the availability bitmap
- `404` event not found

### `GET /events/{id}/availability` (auth)
Only for users admitted from the waiting room (their token for the event is `ACTIVE`), so the queued crowd never polls it. Reads Redis only.
- `200`, `application/octet-stream`: the seat bitmap, bit `i` (most significant bit of each byte first) set when seat `i` is held or sold. Header `X-Available-Count` is the number of free seats. Sends an `ETag`; revalidate with `If-None-Match`.
- `304` unchanged since the given `ETag`
- `403` not admitted, `404` no seat map

### `POST /events/{id}/close`
Ends the sale now instead of at `start_time`. Final and idempotent: joins then get `410`, waiting users see `CLOSED`, the admission worker stops.
- `200 {"event_id", "status": "CLOSED"}`
- `404` event not found

## Waiting Room

### `POST /waiting-room/{event}/join` (auth)
Idempotent: returns the caller's current token while it is in play. Before `sale_opens_at` the token waits in a lobby that is shuffled into a random order when the sale opens, so joining first before the opening gives no advantage; after that, first come, first served. Once a turn is over (the reservation was released, expired or paid, or the token expired unused), joining again queues a new token at the back of the line.
- `202 {"token_id", "status", "seq"?}`: `status` is `LOBBY` or `QUEUED` (or the current token's status); `seq` is the place in line, absent in the lobby
- `404` event not found, `410` sale closed

### `GET /waiting-room/{event}/status` (auth)
The caller's token, found by user; one Redis round trip, meant to be polled.
- `200 {"token_id", "status", ...}` with, by status:
  - `LOBBY`: `sale_opens_at`
  - `QUEUED`: `position` (1 is next), `estimated_wait_secs` (absent until measured, or while admission is stalled), and `reason` when stalled: `all_seats_held` (every free seat is held; wait for holds to end) or `sold_out` (every seat is paid for; only a cancellation would reopen it)
  - `ACTIVE`: `expires_in_ms`, the time left to reserve
  - `USED`: the turn was spent on a reservation; `can_rejoin` once that reservation is over
  - `EXPIRED`: the token ran out; `can_rejoin`
  - `CLOSED`: the sale closed while waiting
- `404` never joined

## Reservations

### `POST /reserve` (auth)
Body `{"event_id", "seat_indexes": [...]}`. Holds the seats for `HOLD_TTL_SECS`, all or none. Needs an `ACTIVE` token; a successful hold uses it up (the next person in line is admitted). A refusal (seat taken, limit, invalid seat) leaves the token `ACTIVE` to try again. Redis only.
- `201 {"reservation_id", "expires_at"}`
- `200` the same, for a retry after the hold was made
- `400` empty, repeated or too many seats, or a seat not in the event
- `403` not admitted, or the turn was already used
- `409` a seat is taken, or the user would exceed `MAX_SEATS_PER_USER` for the event
- `410` sale closed

### `DELETE /reserve/{reservation_id}` (auth)
Releases the caller's hold; the seats are free at once. The turn stays used.
- `204`
- `403` not the caller's, `404` no live hold, `409` payment has started

## Payment

### `POST /pay/{reservation_id}` (auth)
Header `Idempotency-Key` (1-255 characters). Extends the hold once by `PAYMENT_EXTENSION_SECS` (after this it cannot be released), records the booking (`PAYMENT_PENDING`, price locked) and creates the payment intent. Every retry returns the same booking and intent. The outcome arrives from the provider: the payment worker then confirms the booking (`CONFIRMED`, seats `SOLD`) or, if the hold ran out first, refunds it (`REFUNDED`).
- `200 {"reservation_id", "booking_id", "payment_intent_id", "amount", "expires_at"}`
- `400` missing key, `403` not the caller's, `409` already paid, `410` hold expired or released

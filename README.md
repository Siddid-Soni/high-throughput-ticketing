# Conference Waitlist API

A conference booking service with waitlist management, built with Rust (Actix-Web, Diesel), PostgreSQL, and RabbitMQ.

## Overview

- Users register with a set of interest topics; conferences are created with a fixed number of slots.
- A booking is confirmed immediately when a slot is free and no one is waiting; otherwise it joins a FIFO waitlist.
- When a confirmed booking is canceled, the next waitlisted booking is promoted to `ConfirmationPending` and must be confirmed by its owner within 20 seconds. An unconfirmed promotion returns to the end of the waitlist and the next booking is promoted.
- When a conference starts, all remaining waitlisted and pending bookings are canceled.

PostgreSQL is the source of truth for booking state. RabbitMQ is used for delayed events, implemented as TTL queues that dead-letter into consumer queues:

| Timer queue               | Dead-letters to        | Handler                                  |
|---------------------------|------------------------|------------------------------------------|
| `confirmation.timer`      | `confirmation.expired` | Expire a pending confirmation, promote next |
| `conference.start.timer`  | `conference.starts`    | Cancel open waitlist entries at start    |

## Booking States

```
WAITLISTED -> ConfirmationPending -> CONFIRMED
     ^                |
     +---- timeout ---+
Any active state -> CANCELED
```

## API

| Method | Path                                 | Description                         |
|--------|--------------------------------------|-------------------------------------|
| POST   | `/user`                              | Register a user with topics         |
| POST   | `/conference`                        | Create a conference                 |
| POST   | `/book`                              | Book a conference or join waitlist  |
| GET    | `/booking/{id}`                      | Booking status                      |
| POST   | `/confirm`                           | Confirm a promoted booking (owner only) |
| POST   | `/cancel`                            | Cancel a booking                    |
| GET    | `/conference/{name}/bookings`        | List bookings for a conference      |

Request and response formats are documented in [API_REFERENCE.md](API_REFERENCE.md).

## Running

Requirements: Rust (stable), Docker, `diesel_cli` with the PostgreSQL feature.

```bash
docker-compose up -d
diesel migration run
cargo run
```

The server listens on `127.0.0.1:8080`. `DATABASE_URL` is read from `.env`. RabbitMQ is expected at `localhost:5672` with the default `guest` credentials.

## Tests

The integration tests run against a live server:

```bash
pip install -r requirements.txt
python3 all_tests.py
python3 test_multiple_cancellations_waitlist.py
```

## Project Layout

```
src/main.rs      HTTP handlers and server setup
src/actions.rs   Database operations
src/queue.rs     RabbitMQ setup, publishers, and consumers
src/models.rs    Database and API types
src/schema.rs    Diesel schema
migrations/      Database migrations
```

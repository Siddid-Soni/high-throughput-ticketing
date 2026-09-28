# Load Test Results

Appended by `load/final.sh` (step 8.3).

## 2026-09-28 21:39 UTC: 500 users, 100 seats

Pay ratio 0.7, hold 20 s + 20 s for payment, token 60 s, fake payment 1000 ms. Release builds on one machine, one API process and one of each worker; Postgres, Redis and Kafka in Docker.

Finished in 109.5s

Outcomes:
  Paid: 100
  Abandoned: 36
  SoldOut: 363
  NoSeat: 1

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 500 | 5 | 0.7 | 1.0 | 1.2 | 1.6 | 202: 500 |
| status | 16442 | 150 | 0.6 | 1.0 | 1.2 | 6.0 | 200: 16442 |
| availability | 190 | 2 | 0.4 | 0.8 | 0.9 | 1.1 | 200: 190 |
| reserve | 190 | 2 | 0.6 | 1.1 | 5.7 | 6.2 | 201: 136, 409: 54 |
| pay | 100 | 1 | 4.8 | 10.7 | 11.4 | 11.5 | 200: 100 |

Server errors and network failures: 0

Bookings: CONFIRMED 100. Holds still queued in Redis (all events): 0.

- [x] seats sold, at most 100 (100)
- [x] bookings holding more seats than they reserved (0)
- [x] sold seats whose booking is not CONFIRMED (0)
- [x] PAYMENT_PENDING bookings past expires_at + 5 s (0)
- [x] CONFIRMED bookings equal to users who paid (100)
- [x] Redis sold:{event} equal to seats sold (100)
- [x] Redis available:{event} equal to unsold seats (0)

## 2026-09-28 21:43 UTC: 10000 users, 100 seats

> **Found a bug, since fixed.** Status polls during the lobby shuffle saw tokens already `QUEUED` but not yet published by the admission worker, and published them again: the topic held 15,340 messages for 10,000 users, the burst of concurrent sends caused the one `500` (`OutOfOrderSequenceNumber` from the Kafka client) and the status p99 of 59 ms. `flush_lobby.lua` now marks those tokens `published = lobby`, and the API leaves them to the worker. The next run is after the fix.

Pay ratio 0.7, hold 20 s + 20 s for payment, token 60 s, fake payment 1000 ms. Release builds on one machine, one API process and one of each worker; Postgres, Redis and Kafka in Docker.

Finished in 67.4s

Outcomes:
  Paid: 100
  Abandoned: 30
  SoldOut: 9870

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 10000 | 148 | 0.4 | 0.7 | 1.0 | 5.2 | 202: 10000 |
| status | 249899 | 3708 | 0.3 | 0.7 | 59.1 | 764.0 | 200: 249898, 500: 1 |
| availability | 183 | 3 | 0.2 | 0.5 | 0.6 | 0.8 | 200: 183 |
| reserve | 183 | 3 | 0.4 | 0.8 | 1.5 | 4.3 | 201: 130, 409: 53 |
| pay | 100 | 1 | 3.8 | 8.6 | 9.0 | 9.4 | 200: 100 |

Server errors and network failures: 1

Bookings: CONFIRMED 100. Holds still queued in Redis (all events): 0.

- [x] seats sold, at most 100 (100)
- [x] bookings holding more seats than they reserved (0)
- [x] sold seats whose booking is not CONFIRMED (0)
- [x] PAYMENT_PENDING bookings past expires_at + 5 s (0)
- [x] CONFIRMED bookings equal to users who paid (100)
- [x] Redis sold:{event} equal to seats sold (100)
- [x] Redis available:{event} equal to unsold seats (0)

## 2026-09-28 21:47 UTC: 10000 users, 100 seats

After the fix: 10,000 messages in the topic for 10,000 users, no warnings in the API log.

Pay ratio 0.7, hold 20 s + 20 s for payment, token 60 s, fake payment 1000 ms. Release builds on one machine, one API process and one of each worker; Postgres, Redis and Kafka in Docker.

Finished in 113.4s

Outcomes:
  Paid: 100
  Abandoned: 50
  SoldOut: 9850

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 10000 | 88 | 0.4 | 0.7 | 0.8 | 2.1 | 202: 10000 |
| status | 431586 | 3805 | 0.3 | 0.6 | 0.8 | 12.4 | 200: 431586 |
| availability | 215 | 2 | 0.2 | 0.6 | 0.9 | 1.1 | 200: 215 |
| reserve | 215 | 2 | 0.5 | 1.0 | 5.5 | 5.8 | 201: 150, 409: 65 |
| pay | 100 | 1 | 3.8 | 9.2 | 10.9 | 14.6 | 200: 100 |

Server errors and network failures: 0

Bookings: CONFIRMED 100. Holds still queued in Redis (all events): 0.

- [x] seats sold, at most 100 (100)
- [x] bookings holding more seats than they reserved (0)
- [x] sold seats whose booking is not CONFIRMED (0)
- [x] PAYMENT_PENDING bookings past expires_at + 5 s (0)
- [x] CONFIRMED bookings equal to users who paid (100)
- [x] Redis sold:{event} equal to seats sold (100)
- [x] Redis available:{event} equal to unsold seats (0)

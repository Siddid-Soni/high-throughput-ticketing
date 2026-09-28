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

## Capacity 2026-09-28 21:51 UTC: 2000 users, poll 1000 ms, 20s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 2000 | 95 | 98.5 | 241.0 | 251.3 | 760.3 | 202: 1999, 500: 1 |
| status | 34656 | 1644 | 0.3 | 0.6 | 0.8 | 4.9 | 200: 34656 |

Server errors and network failures: 1

Mid-run: Redis 23778 ops/s at 6.22% CPU, API 13% CPU (100% = one core), generator 9%. Kafka messages 1999, distinct seqs 1999 (users 2000).

## Capacity 2026-09-28 21:53 UTC: 2000 users, poll 1000 ms, 20s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 2000 | 95 | 3.6 | 4.8 | 5.3 | 6.5 | 202: 2000 |
| status | 34965 | 1657 | 0.4 | 0.7 | 0.9 | 8.7 | 200: 34965 |

Server errors and network failures: 0

Mid-run: Redis 24068 ops/s at 6.14% CPU, API 19% CPU (100% = one core), generator 15%. Kafka messages 2000, distinct seqs 2000 (users 2000).

## Capacity 2026-09-28 21:53 UTC: 10000 users, poll 2000 ms, 20s, 1 API

Joins within 1000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 10000 | 451 | 3.1 | 4.5 | 5.3 | 6.9 | 202: 10000 |
| status | 97417 | 4390 | 0.3 | 0.7 | 1.2 | 10.2 | 200: 97417 |

Server errors and network failures: 0

Mid-run: Redis 60226 ops/s at 11.07% CPU, API 39% CPU (100% = one core), generator 32%. Kafka messages 10000, distinct seqs 10000 (users 10000).

## Capacity 2026-09-28 21:54 UTC: 20000 users, poll 1000 ms, 30s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 20000 | 643 | 3.2 | 5.0 | 5.8 | 11.7 | 202: 20000 |
| status | 548339 | 17631 | 0.9 | 2.2 | 5.0 | 26.1 | 200: 548339 |

Server errors and network failures: 0

Mid-run: Redis 239311 ops/s at 72.93% CPU, API 125% CPU (100% = one core), generator 99%. Kafka messages 20000, distinct seqs 20000 (users 20000).

## Capacity 2026-09-28 21:55 UTC: 50000 users, poll 1000 ms, 30s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 50000 | 1607 | 16.3 | 4761.0 | 5090.7 | 5406.2 | 0: 3842, 202: 46154, 408: 4 |
| status | 994209 | 31955 | 51.8 | 324.9 | 3733.1 | 5115.1 | 0: 13092, 200: 981106, 408: 11 |

Server errors and network failures: 16934

Mid-run: Redis 481429 ops/s at 96.54% CPU, API 230% CPU (100% = one core), generator 576%. Kafka messages 46154, distinct seqs 46154 (users 50000).

## Capacity 2026-09-28 21:56 UTC: 35000 users, poll 1000 ms, 30s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 35000 | 1125 | 781.4 | 2762.1 | 2956.6 | 3203.7 | 0: 1211, 202: 33309, 500: 480 |
| status | 813104 | 26140 | 1.5 | 213.6 | 1640.6 | 3183.8 | 0: 789, 200: 811902, 500: 413 |

Server errors and network failures: 2893

Mid-run: Redis 384021 ops/s at 91.78% CPU, API 200% CPU (100% = one core), generator 305%. Kafka messages 33464, distinct seqs 33464 (users 35000).

## Capacity 2026-09-28 21:56 UTC: 35000 users, poll 1000 ms, 30s, 2 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 35000 | 1125 | 3.9 | 188.1 | 220.1 | 229.9 | 202: 35000 |
| status | 956069 | 30735 | 1.3 | 15.5 | 94.5 | 114.8 | 200: 956069 |

Server errors and network failures: 0

Mid-run: Redis 418042 ops/s at 78.61% CPU, API 234% CPU (100% = one core), generator 171%. Kafka messages 35000, distinct seqs 35000 (users 35000).

## Capacity 2026-09-28 21:58 UTC: 35000 users, poll 1000 ms, 30s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 35000 | 1125 | 3.8 | 80.7 | 121.8 | 140.6 | 202: 35000 |
| status | 958264 | 30808 | 1.3 | 9.0 | 37.5 | 76.0 | 200: 958264 |

Server errors and network failures: 0

Mid-run: Redis 412504 ops/s at 76.86% CPU, API 163% CPU (100% = one core), generator 161%. Kafka messages 35000, distinct seqs 35000 (users 35000).

## Capacity 2026-09-28 21:58 UTC: 50000 users, poll 1000 ms, 30s, 1 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 50000 | 1592 | 6.2 | 3251.0 | 4926.1 | 7412.5 | 0: 67, 202: 49713, 500: 220 |
| status | 387107 | 12327 | 166.1 | 4909.5 | 8644.6 | 15605.4 | 0: 96409, 200: 284721, 408: 92, 500: 5885 |

Server errors and network failures: 102581

Mid-run: Redis 121644 ops/s at 35.39% CPU, API 142% CPU (100% = one core), generator 1163%. Kafka messages 49832, distinct seqs 49832 (users 50000).

## Capacity 2026-09-28 21:59 UTC: 50000 users, poll 1000 ms, 30s, 2 API

Joins within 5000 ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included.

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 50000 | 1589 | 36.0 | 911.4 | 935.7 | 970.2 | 202: 49907, 500: 93 |
| status | 1174387 | 37316 | 144.9 | 384.7 | 498.2 | 518.1 | 0: 35, 200: 1163326, 500: 11026 |

Server errors and network failures: 11154

Mid-run: Redis 528283 ops/s at 95.51% CPU, API 257% CPU (100% = one core), generator 245%. Kafka messages 49938, distinct seqs 49938 (users 50000).

## Capacity summary (2026-09-28)

The capacity runs above (`load/capacity.sh`) in order, with what each changed. Everything ran on one machine (i9-12900H, 20 threads): one Redis, the API processes, the admission worker and the load generator. Users join an event already on sale, then poll their status once a second.

| Run | Users | APIs | status req/s | status p99 | join p99 | Errors | Redis CPU | Notes |
|---|---|---|---|---|---|---|---|---|
| 1 | 2,000 | 1 | 1,600 | 0.8 ms | 251 ms | 1 | 6% | Before fix 1 |
| 2 | 2,000 | 1 | 1,700 | 0.9 ms | 5.3 ms | 0 | 6% | Fix 1 |
| 3 | 10,000 (joins within 1 s) | 1 | 4,400 | 1.2 ms | 5.3 ms | 0 | 11% | Join burst |
| 4 | 20,000 | 1 | 17,600 | 5.0 ms | 5.8 ms | 0 | 73% | |
| 5 | 50,000 | 1 | 32,000 | 3,733 ms | 5,091 ms | 16,934 | 97% | Before fix 2 |
| 6 | 35,000 | 1 | 26,100 | 1,641 ms | 2,957 ms | 2,893 | 92% | Before fix 2 |
| 7 | 35,000 | 2 | 30,700 | 94 ms | 220 ms | 0 | 79% | Before fix 2 |
| 8 | 35,000 | 1 | 30,800 | 37.5 ms | 122 ms | 0 | 77% | Fix 2 |
| 9 | 50,000 | 1 | 12,300 | 8,645 ms | 4,926 ms | 102,581 | 35% | Load generator collapsed (11.6 cores); not a server result |
| 10 | 50,000 | 2 | 37,300 | 498 ms | 936 ms | 11,154 | 96% | Redis saturated |

Fixes found by these runs:

1. **Kafka producer.** kafkit-client's idempotent producer rejected its own batches (`OutOfOrderSequenceNumber`) once many joins were in flight, and sent one request per join (join p50 98 ms in a 2,000-join burst). Idempotence is off (a token's place comes from its own ack; a duplicate is skipped by admission) and a 2 ms linger batches concurrent joins: join p50 3.6 ms, no errors.
2. **Redis connections.** Each API process used one multiplexed Redis connection; at 35,000 users it became the queue (`redis: timed out`, `500`s) with Redis and the API CPU both below their limits. The API now spreads its worker threads over `REDIS_CONNECTIONS` (8) connections: one process went from 2,893 errors and a 1.6 s p99 to none and 37.5 ms.

**Ceiling: about 30,000-37,000 status polls per second, set by Redis.** A status poll is one Lua script of about 14 Redis commands, and one Redis core saturates near 500,000 commands per second (run 10). Below that, latency is flat (p99 ≤ 5 ms up to 20,000 users, 37.5 ms at 35,000); at it, requests start timing out. Joins stay correct throughout: every run has one Kafka message and one distinct place in line per successful join.

To go further: poll less often the further back a user is (position 30,000 does not need a 1-second refresh; this is the cheapest and biggest win), trim `status.lua` (it reads counters a queued user rarely needs), serve status from Redis replicas, or put events on separate Redis instances. A load generator on a separate machine would also give cleaner numbers above 35,000 users.

## 2026-09-28 22:02 UTC: 10000 users, 100 seats

Pay ratio 0.7, hold 20 s + 20 s for payment, token 60 s, fake payment 1000 ms. Release builds on one machine, one API process and one of each worker; Postgres, Redis and Kafka in Docker.

Finished in 89.8s

Outcomes:
  Paid: 100
  Abandoned: 43
  SoldOut: 9857

| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |
|---|---|---|---|---|---|---|---|
| join | 10000 | 111 | 0.3 | 0.6 | 0.7 | 2.1 | 202: 10000 |
| status | 338553 | 3769 | 0.3 | 0.6 | 1.0 | 8.3 | 200: 338553 |
| availability | 199 | 2 | 0.2 | 0.4 | 0.6 | 0.6 | 200: 199 |
| reserve | 199 | 2 | 0.3 | 0.7 | 1.3 | 6.2 | 201: 143, 409: 56 |
| pay | 100 | 1 | 3.3 | 8.4 | 9.4 | 11.4 | 200: 100 |

Server errors and network failures: 0

Bookings: CONFIRMED 100. Holds still queued in Redis (all events): 0.

- [x] seats sold, at most 100 (100)
- [x] bookings holding more seats than they reserved (0)
- [x] sold seats whose booking is not CONFIRMED (0)
- [x] PAYMENT_PENDING bookings past expires_at + 5 s (0)
- [x] CONFIRMED bookings equal to users who paid (100)
- [x] Redis sold:{event} equal to seats sold (100)
- [x] Redis available:{event} equal to unsold seats (0)

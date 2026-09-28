#!/usr/bin/env bash
# Step 8.3: N users (default 10,000) try to buy one of 100 seats; admitted
# users reserve a random free seat, PAY_RATIO of them pay and the rest walk
# away. Then checks the outcome in Postgres and Redis and appends a report to
# load/RESULTS.md.
#
# Needs `docker compose up -d` (Postgres, Redis, Kafka). Runs release builds
# of the API and workers itself, with short holds so abandoned seats come
# back quickly; stops the compose `app` containers first to free port 8080.
# Usage: load/final.sh [N]

set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-10000}
SEATS=100
export PAY_RATIO=${PAY_RATIO:-0.7}
export HOLD_TTL_SECS=${HOLD_TTL_SECS:-20} PAYMENT_EXTENSION_SECS=${PAYMENT_EXTENSION_SECS:-20}
export TOKEN_TTL_SECS=${TOKEN_TTL_SECS:-60} FAKE_PAYMENT_DELAY_MS=${FAKE_PAYMENT_DELAY_MS:-1000}
API=http://localhost:8080
H='Content-Type: application/json'
R=$RANDOM$RANDOM
WORK=$(mktemp -d)
PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT

psql_() { docker compose exec -T postgres psql -U actix -d conferences -tAc "$1" </dev/null; }
rcli() { docker compose exec -T redis redis-cli "$@" </dev/null; }

echo "Building"
cargo build -q --release --bins
cargo build -q --release --manifest-path load/final/Cargo.toml
docker compose --profile app stop api admission-worker payment-worker cleanup-worker >/dev/null 2>&1 || true

# Request logging off: it would dominate the API's work at this rate
RUST_LOG=warn ./target/release/conference > "$WORK/api.log" 2>&1 & PIDS+=($!)
until curl -s -o /dev/null $API/auth/me; do sleep 0.5; done

echo "Setting up $N users and an event with $SEATS seats"
curl -sf -XPOST $API/auth/register -H "$H" -d "{\"username\":\"load${R}_1\",\"password\":\"password123\"}" >/dev/null
psql_ "INSERT INTO users (username, password_hash)
       SELECT 'load${R}_' || g, (SELECT password_hash FROM users WHERE username = 'load${R}_1')
       FROM generate_series(2, $N) g" >/dev/null
psql_ "SELECT id FROM users WHERE username LIKE 'load${R}\_%' ORDER BY id" > "$WORK/users"
V=$(curl -sf -XPOST $API/venue -H "$H" -d "{\"name\":\"load$R\",\"location\":\"x\",\"seat_map_config\":{\"sections\":[{\"name\":\"A\",\"rows\":10,\"seats_per_row\":10}]}}" | jq .id)
E=$(curl -sf -XPOST $API/events -H "$H" -d "{\"venue_id\":$V,\"name\":\"load$R\",\"start_time\":\"$(date -u -d '+1 day' +%FT%TZ)\",\"sale_opens_at\":\"$(date -u -d '+15 seconds' +%FT%TZ)\",\"prices\":{\"A\":5000}}" | jq .id)

./target/release/admission_worker "$E" > "$WORK/admission.log" 2>&1 & PIDS+=($!)
./target/release/payment_worker > "$WORK/payment.log" 2>&1 & PIDS+=($!)
./target/release/cleanup_worker > "$WORK/cleanup.log" 2>&1 & PIDS+=($!)

echo "Running (the sale opens 15 s after the event was created)"
BASE_URL=$API EVENT_ID=$E SEAT_COUNT=$SEATS USER_IDS="$WORK/users" \
  JWT_SECRET=$(grep '^JWT_SECRET=' .env | cut -d= -f2-) \
  ./load/final/target/release/final-load | tee "$WORK/report" || true

# Let the last abandoned or unpaid holds expire and the payment worker finish
sleep $((HOLD_TTL_SECS + PAYMENT_EXTENSION_SECS + 5))

SOLD=$(psql_ "SELECT count(*) FROM event_seats WHERE event_id = $E AND status = 'SOLD'")
OVER=$(psql_ "SELECT count(*) FROM (SELECT booking_id, count(*) AS n FROM event_seats
              WHERE event_id = $E AND booking_id IS NOT NULL GROUP BY booking_id) s
              WHERE n > (SELECT count(*) FROM booking_seats b WHERE b.booking_id = s.booking_id)")
UNCONFIRMED=$(psql_ "SELECT count(*) FROM event_seats s JOIN bookings b ON b.id = s.booking_id
                     WHERE s.event_id = $E AND b.status <> 'CONFIRMED'")
STUCK=$(psql_ "SELECT count(*) FROM bookings WHERE event_id = $E AND status = 'PAYMENT_PENDING'
               AND expires_at < now() - interval '5 seconds'")
CONFIRMED=$(psql_ "SELECT count(*) FROM bookings WHERE event_id = $E AND status = 'CONFIRMED'")
BY_STATUS=$(psql_ "SELECT string_agg(status || ' ' || n, ', ') FROM (SELECT status::text, count(*) AS n FROM bookings WHERE event_id = $E GROUP BY status ORDER BY status) s")
PAID=$(grep -oP '^  Paid: \K[0-9]+' "$WORK/report" || echo 0)
R_SOLD=$(rcli GET sold:$E); R_AVAILABLE=$(rcli GET available:$E); R_HOLDS=$(rcli ZCARD holds:expiry)

check() { if [ "$2" = "$3" ]; then echo "- [x] $1 ($2)"; else echo "- [ ] **FAILED** $1: $2, expected $3"; FAIL=1; fi; }
FAIL=0
CHECKS=$(
  check "seats sold, at most $SEATS" "$SOLD" "$([ "$SOLD" -le $SEATS ] && echo "$SOLD" || echo "<= $SEATS")"
  check "bookings holding more seats than they reserved" "$OVER" 0
  check "sold seats whose booking is not CONFIRMED" "$UNCONFIRMED" 0
  check "PAYMENT_PENDING bookings past expires_at + 5 s" "$STUCK" 0
  check "CONFIRMED bookings equal to users who paid" "$CONFIRMED" "$PAID"
  check "Redis sold:{event} equal to seats sold" "$R_SOLD" "$SOLD"
  check "Redis available:{event} equal to unsold seats" "$R_AVAILABLE" "$((SEATS - SOLD))"
)
echo "$CHECKS"; grep -q FAILED <<<"$CHECKS" && FAIL=1

{
  echo
  echo "## $(date -u +%F\ %H:%M) UTC: $N users, $SEATS seats"
  echo
  echo "Pay ratio $PAY_RATIO, hold ${HOLD_TTL_SECS} s + ${PAYMENT_EXTENSION_SECS} s for payment, token ${TOKEN_TTL_SECS} s, fake payment ${FAKE_PAYMENT_DELAY_MS} ms. Release builds on one machine, one API process and one of each worker; Postgres, Redis and Kafka in Docker."
  echo
  sed -n '/^Finished/,$p' "$WORK/report"
  echo
  echo "Bookings: ${BY_STATUS}. Holds still queued in Redis (all events): $R_HOLDS."
  echo
  echo "$CHECKS"
} >> load/RESULTS.md
echo "Appended to load/RESULTS.md"
exit $FAIL

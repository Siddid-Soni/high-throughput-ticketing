#!/usr/bin/env bash
# Capacity runs: USERS join an event that is already on sale (within
# JOIN_SPREAD_MS, a burst), then poll their status every POLL_MS for
# DURATION seconds. Reports latency and throughput per endpoint, samples
# Redis, API and generator CPU mid-run, and checks the Kafka topic holds one
# message per user. Appends a summary to load/RESULTS.md.
#
# Needs `docker compose up -d`. Usage:
#   load/capacity.sh USERS POLL_MS DURATION [API_PROCESSES] [JOIN_SPREAD_MS]

set -euo pipefail
cd "$(dirname "$0")/.."

USERS=$1 POLL_MS=$2 DURATION=$3 APIS=${4:-1} JOIN_SPREAD_MS=${5:-5000}
H='Content-Type: application/json'
R=$RANDOM$RANDOM
WORK=$(mktemp -d)
PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT
rcli() { docker compose exec -T redis redis-cli "$@" </dev/null; }

cargo build -q --release --bins
cargo build -q --release --manifest-path load/final/Cargo.toml
docker compose --profile app stop api admission-worker payment-worker cleanup-worker >/dev/null 2>&1 || true

BASES=()
for i in $(seq 0 $((APIS - 1))); do
  RUST_LOG=warn BIND_ADDR=127.0.0.1:$((8080 + i)) ./target/release/conference > "$WORK/api$i.log" 2>&1 & PIDS+=($!)
  BASES+=("http://127.0.0.1:$((8080 + i))")
done
API_PIDS="${PIDS[*]}"
for b in "${BASES[@]}"; do until curl -s -o /dev/null "$b/auth/me"; do sleep 0.5; done; done

V=$(curl -sf -XPOST ${BASES[0]}/venue -H "$H" -d "{\"name\":\"cap$R\",\"location\":\"x\",\"seat_map_config\":{\"sections\":[{\"name\":\"A\",\"rows\":10,\"seats_per_row\":10}]}}" | jq .id)
E=$(curl -sf -XPOST ${BASES[0]}/events -H "$H" -d "{\"venue_id\":$V,\"name\":\"cap$R\",\"start_time\":\"$(date -u -d '+1 day' +%FT%TZ)\",\"sale_opens_at\":\"$(date -u -d '+2 seconds' +%FT%TZ)\",\"prices\":{\"A\":5000}}" | jq .id)
./target/release/admission_worker "$E" > "$WORK/admission.log" 2>&1 & PIDS+=($!)
until grep -q "Lobby flushed" "$WORK/admission.log"; do sleep 0.5; done

echo "$USERS users, poll every ${POLL_MS} ms for ${DURATION}s, $APIS API process(es), joins within ${JOIN_SPREAD_MS} ms"
MODE=poll USERS=$USERS POLL_MS=$POLL_MS DURATION_SECS=$DURATION JOIN_SPREAD_MS=$JOIN_SPREAD_MS \
  BASE_URL=$(IFS=,; echo "${BASES[*]}") EVENT_ID=$E SEAT_COUNT=100 \
  JWT_SECRET=$(grep '^JWT_SECRET=' .env | cut -d= -f2-) \
  ./load/final/target/release/final-load > "$WORK/report" 2>&1 & GEN=$!

# Mid-run: steady polling, joins done
sleep $((DURATION / 2 + 2))
REDIS_OPS=$(rcli INFO stats | grep -oP 'instantaneous_ops_per_sec:\K[0-9]+')
REDIS_CPU=$(docker stats --no-stream --format '{{.CPUPerc}}' "$(docker compose ps -q redis)")
API_CPU=$(ps -o %cpu= -p "${API_PIDS// /,}" | awk '{s+=$1} END {printf "%.0f%%", s}')
GEN_CPU=$(ps -o %cpu= -p $GEN | awk '{printf "%.0f%%", $1}')
wait $GEN || true
cat "$WORK/report"

MESSAGES=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic waiting.$E 2>/dev/null </dev/null | cut -d: -f3)
UNIQUE_SEQ=$(rcli EVAL "local seen, n = {}, 0 for _, k in ipairs(redis.call('KEYS', 'user_token:$E:*')) do local s = redis.call('HGET', 'token:' .. redis.call('GET', k), 'seq') if s and not seen[s] then seen[s] = true n = n + 1 end end return n" 0)
SAMPLE="Mid-run: Redis ${REDIS_OPS} ops/s at ${REDIS_CPU} CPU, API ${API_CPU} CPU (100% = one core), generator ${GEN_CPU}. Kafka messages ${MESSAGES}, distinct seqs ${UNIQUE_SEQ} (users ${USERS})."
echo "$SAMPLE"

{
  echo
  echo "## Capacity $(date -u +%F\ %H:%M) UTC: $USERS users, poll ${POLL_MS} ms, ${DURATION}s, $APIS API"
  echo
  echo "Joins within ${JOIN_SPREAD_MS} ms after the sale opened, then status polls; all on one 20-thread machine (i9-12900H), the load generator included."
  echo
  sed -n '/^|/p' "$WORK/report"
  echo
  grep '^Server errors' "$WORK/report" || true
  echo
  echo "$SAMPLE"
} >> load/RESULTS.md

#!/usr/bin/env bash
# Step 5.4: N admitted users all reserve the same seat at the same moment.
# Expect exactly one 201 and N-1 409s, one seat held, and nothing written to
# Postgres (holds live in Redis until payment).
#
# Needs the API on localhost:8080 and `docker compose up -d`. Starts its own
# admission worker for the event. Usage: load/reserve_race.sh [N]

set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-1000}
API=localhost:8080
H='Content-Type: application/json'
R=$RANDOM$RANDOM
WORK=$(mktemp -d)
WORKER=
trap 'kill $WORKER 2>/dev/null || true; rm -rf "$WORK"' EXIT

psql_() { docker compose exec -T postgres psql -U actix -d conferences -tAc "$1" </dev/null; }
rcli() { docker compose exec -T redis redis-cli "$@" </dev/null; }

# curl --parallel with one request per `next` block: every request carries its own token
# (the last block's `next` is dropped: an empty block is an error)
parallel_curl() { sed -i '$d' "$1"; curl -s --parallel --parallel-immediate --parallel-max "$N" -K "$1"; }

echo "Setting up an event with $N seats and $N users"
V=$(curl -sf -XPOST $API/venue -H "$H" -d "{\"name\":\"race$R\",\"location\":\"x\",\"seat_map_config\":{\"sections\":[{\"name\":\"A\",\"rows\":$(( (N + 99) / 100 )),\"seats_per_row\":100}]}}" | jq .id)
E=$(curl -sf -XPOST $API/events -H "$H" -d "{\"venue_id\":$V,\"name\":\"race$R\",\"start_time\":\"$(date -u -d '+1 day' +%FT%TZ)\",\"sale_opens_at\":\"$(date -u -d '+1 hour' +%FT%TZ)\",\"prices\":{\"A\":1000}}" | jq .id)

# Register one user through the API, then copy its password hash to the rest
curl -sf -XPOST $API/auth/register -H "$H" -d "{\"username\":\"race${R}_1\",\"password\":\"password123\"}" >/dev/null
psql_ "INSERT INTO users (username, password_hash)
       SELECT 'race${R}_' || g, (SELECT password_hash FROM users WHERE username = 'race${R}_1')
       FROM generate_series(2, $N) g" >/dev/null

seq 1 "$N" | xargs -P 20 -I{} curl -sf -XPOST $API/auth/login -H "$H" \
  -d "{\"username\":\"race${R}_{}\",\"password\":\"password123\"}" -o "$WORK/jwt.{}"
for i in $(seq 1 "$N"); do jq -r .token "$WORK/jwt.$i"; done > "$WORK/jwts"

# Everyone joins the lobby, then the sale opens now and the worker admits them all
: > "$WORK/join.cfg"
while read -r T; do
  printf 'url = "http://%s/waiting-room/%s/join"\nrequest = "POST"\nheader = "Authorization: Bearer %s"\noutput = "/dev/null"\nnext\n' $API "$E" "$T" >> "$WORK/join.cfg"
done < "$WORK/jwts"
parallel_curl "$WORK/join.cfg"
psql_ "UPDATE events SET sale_opens_at = now() WHERE id = $E" >/dev/null
MAX_ACTIVE_TOKENS=$((N * 2)) ./target/debug/admission_worker "$E" > "$WORK/worker.log" 2>&1 &
WORKER=$!
until [ "$(rcli ZCARD active_tokens:$E)" -ge "$N" ]; do sleep 0.5; done
echo "All $N users are ACTIVE"

# The race: everyone reserves seat 0 at once
: > "$WORK/reserve.cfg"
while read -r T; do
  printf 'url = "http://%s/reserve"\nrequest = "POST"\nheader = "Authorization: Bearer %s"\nheader = "%s"\ndata = "{\\"event_id\\":%s,\\"seat_indexes\\":[0]}"\noutput = "/dev/null"\nwrite-out = "%%{http_code}\\n"\nnext\n' $API "$T" "$H" "$E" >> "$WORK/reserve.cfg"
done < "$WORK/jwts"
START=$(date +%s%N)
parallel_curl "$WORK/reserve.cfg" > "$WORK/codes"
END=$(date +%s%N)

echo "Responses ($(awk "BEGIN { printf \"%.2f\", ($END - $START) / 1e9 }")s):"
sort "$WORK/codes" | uniq -c
BOOKINGS=$(psql_ "SELECT count(*) FROM bookings WHERE event_id = $E")
AVAILABLE=$(rcli GET available:$E)
echo "Bookings written: $BOOKINGS   seat 0 held: $(rcli GETBIT seats:$E 0)   available: $AVAILABLE of $N"

CREATED=$(grep -c '^201$' "$WORK/codes" || true)
CONFLICT=$(grep -c '^409$' "$WORK/codes" || true)
if [ "$CREATED" -eq 1 ] && [ "$CONFLICT" -eq $((N - 1)) ] && [ "$BOOKINGS" -eq 0 ] && [ "$AVAILABLE" -eq $((N - 1)) ]; then
  echo "PASS"
else
  echo "FAIL"; exit 1
fi

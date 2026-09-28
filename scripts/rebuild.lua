-- Rebuild an event's seat map and counters after Redis lost them, atomically.
-- Runs only while `seat_count:{e}` is missing: joins and reserves refuse an
-- event without it, so no hold can be made while the inputs are gathered,
-- and it is written last (in this same call), which reopens the event.
-- KEYS: 1 seats:{e}  2 available:{e}  3 seat_count:{e}  4 sold:{e}
--       5 sale_opens_at:{e}  6 closed:{e}  7 lobby_flushed:{e}
-- ARGV: 1 event id  2 seat count  3 sold count  4 sale_opens_at
--       5 closed (0/1)  6 on sale (0/1)
--       7 JSON {taken: [seat indexes sold or blocked in Postgres],
--               holds: [reservation ids that were live when scanned],
--               user_seats: {user id: paid seats}}
-- Returns 1 if rebuilt, 0 if the seat map was already there.

if redis.call('EXISTS', KEYS[3]) == 1 then
  return 0
end
local e = ARGV[1]
local n = tonumber(ARGV[2])
local input = cjson.decode(ARGV[7])

redis.call('DEL', KEYS[1])
if n > 0 then
  redis.call('SETBIT', KEYS[1], n - 1, 0)
end
local taken = 0
local function take(index)
  if redis.call('SETBIT', KEYS[1], index, 1) == 0 then
    taken = taken + 1
  end
end
for _, index in ipairs(input.taken) do
  take(index)
end

-- Each user's limit counts paid seats plus live holds. Set, not added to, in
-- case an old counter survived
local per_user = {}
for user, count in pairs(input.user_seats) do
  per_user[tostring(user)] = count
end
for _, id in ipairs(input.holds) do
  -- Re-read: a hold may have been released or expired since the scan
  local raw = redis.call('GET', 'resv:' .. id)
  if raw then
    local resv = cjson.decode(raw)
    for _, index in ipairs(resv.seats) do
      take(index)
    end
    local user = tostring(resv.user)
    per_user[user] = (per_user[user] or 0) + #resv.seats
  end
end
for user, count in pairs(per_user) do
  redis.call('SET', 'user_seats:' .. e .. ':' .. user, count)
end

redis.call('SET', KEYS[2], n - taken)
redis.call('SET', KEYS[4], ARGV[3])
redis.call('SET', KEYS[5], ARGV[4])
if ARGV[5] == '1' then
  redis.call('SET', KEYS[6], 1)
end
-- Once the sale is open, joins go straight to the queue (the lobby is gone)
if ARGV[6] == '1' then
  redis.call('SET', KEYS[7], 1)
end
redis.call('SET', KEYS[3], n)
return 1

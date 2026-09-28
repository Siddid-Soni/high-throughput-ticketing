-- Finish a claimed payment after its Postgres transaction committed: phase 3
-- of 3. Deletes the hold and adjusts one counter in the same call. Seat bits
-- stay set: the seats are sold (or, for a refund after a conflict, sold to
-- someone else).
-- KEYS: 1 resv:{id}
-- ARGV: 1 'sold' or 'refunded'
-- Returns 1, or 0 if already finalized.

local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local resv = cjson.decode(raw)
redis.call('DEL', KEYS[1])
local e = tostring(resv.event)
local n = #resv.seats
if ARGV[1] == 'sold' then
  redis.call('INCRBY', 'sold:' .. e, n)
else
  -- Refunded: these seats no longer count toward the user's limit
  redis.call('DECRBY', 'user_seats:' .. e .. ':' .. tostring(resv.user), n)
end
return 1

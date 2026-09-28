-- Release a seat hold, atomically.
-- KEYS: 1 resv:{id}  2 holds:expiry
-- ARGV: 1 reservation id  2 user id that must own it, or '' for the system
-- Returns 'released', 'not_found' (no live hold), 'forbidden' or 'paying'
-- (payment has started: only expiry or the payment outcome ends it).

local raw = redis.call('GET', KEYS[1])
if not raw then
  return 'not_found'
end
local resv = cjson.decode(raw)
if ARGV[2] ~= '' and tostring(resv.user) ~= ARGV[2] then
  return 'forbidden'
end
if resv.state and resv.state ~= 'held' then
  return 'paying'
end

redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
local e = tostring(resv.event)
for _, index in ipairs(resv.seats) do
  redis.call('SETBIT', 'seats:' .. e, index, 0)
end
local n = #resv.seats
redis.call('INCRBY', 'available:' .. e, n)
redis.call('DECRBY', 'user_seats:' .. e .. ':' .. tostring(resv.user), n)
-- The user's token stays USED: their turn was spent on the reservation
return 'released'

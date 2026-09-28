-- Hold seats for an admitted user, all or nothing, atomically.
-- KEYS: 1 user_token:{e}:{u}  2 seats:{e}  3 available:{e}  4 user_seats:{e}:{u}
--       5 holds:expiry  6 active_tokens:{e}  7 closed:{e}  8 seat_count:{e}
-- ARGV: 1 reservation id  2 user id  3 event id  4 hold ttl ms  5 grace ms
--       6 max seats per user  7.. seat indexes
-- Returns {'ok', resv_id, expires_at_ms}, {'existing', resv_id, expires_at_ms},
-- {'closed'}, {'not_admitted'}, {'turn_used'}, {'invalid_seat', index},
-- {'too_many', seats_held} or {'seat_taken', index}.
--
-- A refusal leaves an ACTIVE token ACTIVE, so a user who loses a seat to a
-- faster buyer can pick again. Only a successful hold uses the token up, and
-- nothing gives it back.

if redis.call('EXISTS', KEYS[7]) == 1 then
  return {'closed'}
end

local token_id = redis.call('GET', KEYS[1])
if not token_id then
  return {'not_admitted'}
end
local token = 'token:' .. token_id
local t = redis.call('HMGET', token, 'status', 'resv')
if t[1] == 'USED' then
  -- A retry after a timeout gets the hold it already made
  local expires_at = t[2] and redis.call('ZSCORE', KEYS[5], t[2])
  if expires_at then
    return {'existing', t[2], expires_at}
  end
  -- The hold was released, expired or paid: the turn is spent
  return {'turn_used'}
end
if t[1] ~= 'ACTIVE' then
  return {'not_admitted'}
end

local seat_count = tonumber(redis.call('GET', KEYS[8]) or '0')
local seats = {}
for i = 7, #ARGV do
  local index = tonumber(ARGV[i])
  if not index or index < 0 or index >= seat_count or index % 1 ~= 0 then
    return {'invalid_seat', ARGV[i]}
  end
  seats[#seats + 1] = index
end
local n = #seats

local held = tonumber(redis.call('GET', KEYS[4]) or '0')
if held + n > tonumber(ARGV[6]) then
  return {'too_many', tostring(held)}
end

for _, index in ipairs(seats) do
  if redis.call('GETBIT', KEYS[2], index) == 1 then
    return {'seat_taken', tostring(index)}
  end
end

-- Everything checked: nothing below can fail halfway
for _, index in ipairs(seats) do
  redis.call('SETBIT', KEYS[2], index, 1)
end
redis.call('DECRBY', KEYS[3], n)
redis.call('INCRBY', KEYS[4], n)

local now_t = redis.call('TIME')
local now = tonumber(now_t[1]) * 1000 + math.floor(tonumber(now_t[2]) / 1000)
local ttl = tonumber(ARGV[4])
local expires_at = now + ttl
local resv = cjson.encode({user = tonumber(ARGV[2]), event = tonumber(ARGV[3]), token = token_id, seats = seats})
-- The key outlives the hold by a grace period: the cleanup worker releases
-- the hold at `expires_at` (holds:expiry), and needs the key to do it
redis.call('SET', 'resv:' .. ARGV[1], resv, 'PX', ttl + tonumber(ARGV[5]))
redis.call('ZADD', KEYS[5], expires_at, ARGV[1])

-- The user's turn is used: free the active slot for the next person in line
redis.call('HSET', token, 'status', 'USED', 'resv', ARGV[1])
redis.call('ZREM', KEYS[6], token_id)
return {'ok', ARGV[1], tostring(expires_at)}

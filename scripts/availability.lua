-- Seat availability for an admitted user, in one round trip. Read-only.
-- KEYS: 1 user_token:{e}:{u}  2 seats:{e}  3 available:{e}
-- Returns {'not_admitted'} unless the caller's token for the event is ACTIVE,
-- {'no_seat_map'} if the event has no bitmap, otherwise
-- {'ok', bitmap, available}.

local id = redis.call('GET', KEYS[1])
if not id or redis.call('HGET', 'token:' .. id, 'status') ~= 'ACTIVE' then
  return {'not_admitted'}
end
local bits = redis.call('GET', KEYS[2])
if not bits then
  return {'no_seat_map'}
end
return {'ok', bits, redis.call('GET', KEYS[3]) or '0'}

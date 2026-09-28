-- Close an event's sale, if the event exists, atomically.
-- KEYS: 1 seat_count:{e}  2 closed:{e}
-- Returns 1 if the event exists (now closed, or already was), 0 if not.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[2], 1)
return 1

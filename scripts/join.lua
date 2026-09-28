-- Join an event's waiting room, atomically.
-- KEYS: 1 closed:{e}  2 seat_count:{e}  3 user_token:{e}:{u}
--       4 lobby_flushed:{e}  5 lobby:{e}
-- ARGV: 1 new token id  2 user id  3 event id
-- Returns {kind, token_id, status, seq, published}; kind is
-- closed | not_found | existing | created.

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {'closed'}
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  return {'not_found'}
end

-- Idempotent: a user keeps one token per event while it is in play. A USED
-- token is in play only while its hold lasts (holding seats or paying); once
-- the hold is released, expires or is paid, joining again queues a new token
-- at the back of the line. Deciding here covers every way a hold can end.
local existing = redis.call('GET', KEYS[3])
if existing then
  local t = redis.call('HMGET', 'token:' .. existing, 'status', 'seq', 'published', 'resv')
  local in_play = t[1] and (t[1] ~= 'USED' or (t[4] and redis.call('EXISTS', 'resv:' .. t[4]) == 1))
  if in_play then
    return {'existing', existing, t[1], t[2] or '', t[3] or ''}
  end
end

local id = ARGV[1]
local token = 'token:' .. id
redis.call('HSET', token, 'user', ARGV[2], 'event', ARGV[3])
redis.call('SET', KEYS[3], id)

-- Before the lobby has been shuffled into the queue (sale not open yet),
-- wait in the lobby without a place in line
if redis.call('EXISTS', KEYS[4]) == 0 then
  redis.call('HSET', token, 'status', 'LOBBY')
  redis.call('SADD', KEYS[5], id)
  return {'created', id, 'LOBBY', '', ''}
end

-- The place in line (seq) is assigned when the caller publishes the token to
-- Kafka: the topic's offset order is the admission order
redis.call('HSET', token, 'status', 'QUEUED')
return {'created', id, 'QUEUED', '', ''}

"""Lua scripts executed atomically on the Redis server.

All barrier mutations go through these scripts so that concurrent check-ins are
serialized by Redis itself: the first script that pushes the checked-in count to
the threshold freezes the quorum snapshot in the same atomic step, and every
subsequent script sees status "met"/"closed" and becomes a no-op.

Server time (TIME) is used inside the scripts so the application and Redis
clocks can never disagree about the deadline.
"""

# KEYS: meta, participants, checked, result, channel
# ARGV: participant_id
# Returns a table whose first element is a status code:
#   NOT_FOUND | CLOSED | EXPIRED | NOT_PARTICIPANT   -> rejected
#   ALREADY_MET                                       -> no-op, barrier already frozen
#   {CHECKED_IN|MET, added, count}                    -> recorded (added: 1 new / 0 duplicate)
CHECKIN_SCRIPT = """
local meta         = KEYS[1]
local participants = KEYS[2]
local checked      = KEYS[3]
local result       = KEYS[4]
local channel      = KEYS[5]
local participant  = ARGV[1]

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local status = redis.call('HGET', meta, 'status')
if not status then
  return {'NOT_FOUND'}
end
if status == 'met' then
  -- Frozen: never rewrite the result.
  return {'ALREADY_MET'}
end
if status == 'closed' then
  return {'CLOSED'}
end

local deadline = tonumber(redis.call('HGET', meta, 'deadline'))
if now >= deadline then
  redis.call('HSET', meta, 'status', 'closed')
  redis.call('PUBLISH', channel, 'closed')
  return {'EXPIRED'}
end

if redis.call('SISMEMBER', participants, participant) == 0 then
  return {'NOT_PARTICIPANT'}
end

local expire_at = redis.call('HGET', meta, 'expire_at')
local added = redis.call('SADD', checked, participant)
if expire_at then
  redis.call('PEXPIREAT', checked, tonumber(expire_at))
end

local count     = redis.call('SCARD', checked)
local threshold = tonumber(redis.call('HGET', meta, 'threshold'))

if count >= threshold then
  -- First arrival at the threshold: atomically freeze the quorum snapshot.
  local members = redis.call('SMEMBERS', checked)
  table.sort(members)
  redis.call('HSET', meta, 'status', 'met', 'met_at', string.format('%.6f', now))
  redis.call('SET', result, cjson.encode(members))
  if expire_at then
    redis.call('PEXPIREAT', result, tonumber(expire_at))
  end
  redis.call('PUBLISH', channel, 'met')
  return {'MET', added, count}
end

return {'CHECKED_IN', added, count}
"""

# KEYS: meta, channel
# Idempotently closes an open barrier whose deadline has passed and wakes waiters.
FINALIZE_SCRIPT = """
local meta    = KEYS[1]
local channel = KEYS[2]

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local status = redis.call('HGET', meta, 'status')
if not status then
  return 'NOT_FOUND'
end
if status == 'open' then
  local deadline = tonumber(redis.call('HGET', meta, 'deadline'))
  if now >= deadline then
    redis.call('HSET', meta, 'status', 'closed')
    redis.call('PUBLISH', channel, 'closed')
    return 'CLOSED'
  end
end
return string.upper(status)
"""

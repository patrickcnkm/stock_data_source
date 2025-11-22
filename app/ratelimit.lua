-- KEYS[1]=key ARGV: now_ms, rate_per_sec, capacity
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cap  = tonumber(ARGV[3])
local ttl  = 60

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if not tokens then
  tokens = cap
  ts = now
else
  local delta = math.max(0, now - ts) / 1000.0
  tokens = math.min(cap, tokens + delta * rate)
  ts = now
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', ts)
redis.call('EXPIRE', key, ttl)
return allowed
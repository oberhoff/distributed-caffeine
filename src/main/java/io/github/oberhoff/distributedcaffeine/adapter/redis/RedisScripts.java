/*
 * Copyright © 2023-2026 Dr. Andreas Oberhoff (All rights reserved)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.oberhoff.distributedcaffeine.adapter.redis;

/**
 * The Lua scripts of the Redis store, one per operation, each atomic (Redis runs one script at a time).
 * <p>
 * Numbers: Lua's numbers are doubles, exact up to 2^53. A core version (milliseconds since 2020 above 20 bits of
 * counter and node, InternalVersion) reaches 2^58, so it never passes through Lua as one number: it is two, the high
 * part (milliseconds, below 2^43) and the low 20 bits, compared and incremented as a pair, and stored as two fields
 * (vh, vl). Write times are microseconds of the server's clock (TIME), below 2^53. Every number is written as a string
 * with string.format('%.0f'), never Lua's default conversion, which keeps 14 significant digits only.
 * <p>
 * A row is a hash: s (state: V, T, E), vh and vl (its version), w (its write time), k (the key), v (the value, for V
 * only). Its hash is in the sorted set of values or of dead rows (tombstones, expired), scored by its write time,
 * unless it is reserved (starting with #).
 * <p>
 * Every write and expiry is announced in the same script, so none goes without its entry and the stream's order is the
 * order of the writes: an entry in the cache's stream, n (its number from the counter, one above the entry before), h
 * (the hash), vh and vl (the version), c (W: written, I: invalidated, E: expired). The stream keeps the entries of the
 * last minutes (trimmed by age, MINID, approximately); a reader that finds a number skipped knows it missed entries
 * (trimmed before it read them, or lost with a failover), which an ID alone would not tell it.
 *
 * @author Andreas Oberhoff
 */
final class RedisScripts {

    private RedisScripts() {
    }

    // the clock, the version arithmetic and the announcement every writing script shares. KEYS[1]: values, KEYS[2]:
    // dead, KEYS[3]: the stream, KEYS[4]: its counter. ARGV[1]: the core's epoch in milliseconds (2020), ARGV[2]: how
    // far a proposal may lead the server's clock in milliseconds, ARGV[3]: how long the stream keeps its entries in
    // milliseconds. A version is assigned as
    // max(stored + 1, floor, min(proposed, floor + lead)), the floor the server's time in the core's format: above
    // the stored one, so it only grows; at least the floor, so a row created anew (deleted by maintenance) starts
    // above what the server issued while its clock went forward; the proposal where it is higher, so the core's own
    // clock carries the key, but not beyond the lead, so no instance's clock far ahead carries it (as MongoStore)
    private static final String PRELUDE = """
            local t = redis.call('TIME')
            local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
            local floor = math.floor(now / 1000) - tonumber(ARGV[1])
            local lead = tonumber(ARGV[2])
            local function num(x) return string.format('%.0f', x) end
            local function less(ah, al, bh, bl) return ah < bh or (ah == bh and al < bl) end
            local function assign(sh, sl, ph, pl)
              local h, l = sh, sl + 1
              if l >= 1048576 then h, l = h + 1, 0 end
              if less(h, l, floor, 0) then h, l = floor, 0 end
              local bh, bl = ph, pl
              if less(floor + lead, 0, bh, bl) then bh, bl = floor + lead, 0 end
              if less(h, l, bh, bl) then h, l = bh, bl end
              return h, l
            end
            local keep = math.floor(now / 1000) - tonumber(ARGV[3])
            local function announce(hash, h, l, kind)
              local n = redis.call('INCR', KEYS[4])
              redis.call('XADD', KEYS[3], 'MINID', '~', num(keep), '*', 'n', n, 'h', hash, 'vh', num(h), 'vl', num(l),
                  'c', kind)
            end
            """;

    // Writes a batch, atomically, announcing each write applied, exactly once per request: KEYS[5] is the request's
    // key ({dc:<name>}:request:<id>, an ID per call of RedisStore#write), under which the script keeps its result for
    // ARGV[4] milliseconds, and a request found there is answered from it without writing. A client library sending
    // the script again after a connection reset (Lettuce does, by default, what it sent and had no answer for) would
    // otherwise apply it twice: an advance at two versions, a conditional write refused by its own result. KEYS[5 + i]:
    // the row of write i. ARGV after ARGV[4], per write, 9 fields: hash, condition (A: advance, M: missing, V: on a
    // version), the expected version's high and low part, the proposed version's high and low part, T for a tombstone
    // or V for a value, the key and the value (both empty for a tombstone, which keeps the row's key, or has none where
    // no row existed). Per write the result is {0} (refused) or {1, high, low} (applied at that version). An advance is
    // never refused: its version is assigned
    static final RedisScript WRITE = RedisScript.of("write", RedisScript.Reply.ARRAY, PRELUDE + """
            local kept = redis.call('GET', KEYS[5])
            if kept then
              local out = {}
              for part in string.gmatch(kept, '[^,]+') do
                if part == '0' then
                  out[#out + 1] = {0}
                else
                  local h, l = string.match(part, '^1:(%d+):(%d+)$')
                  out[#out + 1] = {1, tonumber(h), tonumber(l)}
                end
              end
              return out
            end
            local out, result = {}, {}
            local n = (#ARGV - 4) / 9
            for i = 1, n do
              local a = 4 + (i - 1) * 9
              local hash, condition, tombstone = ARGV[a + 1], ARGV[a + 2], ARGV[a + 7] == 'T'
              local row = KEYS[5 + i]
              local stored = redis.call('HMGET', row, 'vh', 'vl', 'k')
              local exists = stored[1] ~= false
              local refused = false
              if condition == 'M' then
                refused = exists
              elseif condition == 'V' then
                refused = not exists or tonumber(stored[1]) ~= tonumber(ARGV[a + 3])
                    or tonumber(stored[2]) ~= tonumber(ARGV[a + 4])
              end
              if refused then
                out[i] = {0}
                result[i] = '0'
              else
                local sh, sl = 0, 0
                if exists then sh, sl = tonumber(stored[1]), tonumber(stored[2]) end
                local h, l = assign(sh, sl, tonumber(ARGV[a + 5]), tonumber(ARGV[a + 6]))
                local reserved = string.sub(hash, 1, 1) == '#'
                if tombstone then
                  redis.call('HSET', row, 's', 'T', 'vh', num(h), 'vl', num(l), 'w', num(now), 'k', stored[3] or '')
                  redis.call('HDEL', row, 'v')
                  if not reserved then
                    redis.call('ZREM', KEYS[1], hash)
                    redis.call('ZADD', KEYS[2], num(now), hash)
                  end
                  announce(hash, h, l, 'I')
                else
                  redis.call('HSET', row, 's', 'V', 'vh', num(h), 'vl', num(l), 'w', num(now), 'k', ARGV[a + 8],
                      'v', ARGV[a + 9])
                  if not reserved then
                    redis.call('ZREM', KEYS[2], hash)
                    redis.call('ZADD', KEYS[1], num(now), hash)
                  end
                  announce(hash, h, l, 'W')
                end
                out[i] = {1, h, l}
                result[i] = '1:' .. num(h) .. ':' .. num(l)
              end
            end
            redis.call('SET', KEYS[5], table.concat(result, ','), 'PX', ARGV[4])
            return out
            """);

    // Reads rows, every age from one reading of the clock: the server's now, or ARGV[3] where given (the instant of a
    // read's first chunk, for the chunks after it). KEYS: the rows. ARGV[1]: the maximum age in microseconds, ARGV[2]:
    // R for whole rows, else versions only. Returns {now, row 1, row 2, ...}, a row false where missing; a
    // whole row is {state, high, low, write time, key, value or false}, a value row at least the maximum age old as E
    // without its value; versions only are {high, low, write time} of value rows younger than the maximum age, false
    // for any other
    static final RedisScript READ = RedisScript.of("read", RedisScript.Reply.ARRAY, """
            local now
            if ARGV[3] ~= '' then
              now = tonumber(ARGV[3])
            else
              local t = redis.call('TIME')
              now = tonumber(t[1]) * 1000000 + tonumber(t[2])
            end
            local maxAge = tonumber(ARGV[1])
            local whole = ARGV[2] == 'R'
            local out = {now}
            for i = 1, #KEYS do
              local f = redis.call('HMGET', KEYS[i], 's', 'vh', 'vl', 'w', 'k', 'v')
              if f[1] == false then
                out[i + 1] = false
              else
                local s, w, v = f[1], tonumber(f[4]), f[6]
                if s == 'V' and now - w >= maxAge then s, v = 'E', false end
                if whole then
                  out[i + 1] = {s, tonumber(f[2]), tonumber(f[3]), w, f[5], v}
                elseif s == 'V' then
                  out[i + 1] = {tonumber(f[2]), tonumber(f[3]), w}
                else
                  out[i + 1] = false
                end
              end
            end
            return out
            """);

    // the write time of the control row (KEYS[5]), below which every row is hidden ('-inf' where there is none), and
    // the lower bound of the rows it does not hide; shared by the maintenance scripts
    private static final String HIDDEN = """
            local control = redis.call('HGET', KEYS[5], 'w')
            local visible = control and ('(' .. control) or '-inf'
            local function hidden(w) return control and w <= tonumber(control) end
            """;

    // Maintenance in two steps, so that every script declares every key it touches (Redis's rule, which Dragonfly
    // enforces and a cluster will need): CANDIDATES reads the hashes to act on from the sorted sets, the oldest first,
    // up to the limit; a script per operation then gets those rows declared, checks each again and acts on those
    // still due, so that a row changed meanwhile, or handled by another instance's run, is left alone (every row is
    // expired or deleted once however many instances run maintenance). KEYS of every maintenance script: values,
    // dead, the stream, its counter, the control row, then (the acting ones) the rows. ARGV: the prelude's three.

    // The candidates of one operation. ARGV[4]: E (expiry: ARGV[5] the maximum age in microseconds, ARGV[6] the
    // limit), S (pruning to size: ARGV[5] the maximum size, ARGV[6] the minimum age, ARGV[7] the limit) or T (pruning
    // tombstones: ARGV[5] the retention, ARGV[6] the limit). Returns the hashes
    static final RedisScript CANDIDATES = RedisScript.of("candidates", RedisScript.Reply.ARRAY, PRELUDE + HIDDEN + """
            local operation = ARGV[4]
            if operation == 'E' then
              return redis.call('ZRANGEBYSCORE', KEYS[1], visible, num(now - tonumber(ARGV[5])), 'LIMIT', 0,
                  tonumber(ARGV[6]))
            end
            if operation == 'S' then
              local excess = redis.call('ZCOUNT', KEYS[1], visible, '+inf') - tonumber(ARGV[5])
              if excess <= 0 then return {} end
              return redis.call('ZRANGEBYSCORE', KEYS[1], visible, num(now - tonumber(ARGV[6])), 'LIMIT', 0,
                  math.min(excess, tonumber(ARGV[7])))
            end
            local limit, found, seen = tonumber(ARGV[6]), {}, {}
            local function add(set, upper)
              if #found >= limit then return end
              for _, hash in ipairs(redis.call('ZRANGEBYSCORE', set, '-inf', upper, 'LIMIT', 0, limit - #found)) do
                if not seen[hash] then
                  seen[hash] = true
                  found[#found + 1] = hash
                end
              end
            end
            if control then
              add(KEYS[1], control)
              add(KEYS[2], control)
            end
            add(KEYS[2], num(now - tonumber(ARGV[5])))
            return found
            """);

    // Expires the given rows still due: a value at least the maximum age old, not hidden, each at an assigned version
    // (the proposal), its write time now, each announced. ARGV[4]: the maximum age in microseconds, ARGV[5] and
    // ARGV[6]: the proposed version's high and low part, ARGV[6 + i]: the hash of KEYS[5 + i]. Returns the number
    // expired
    static final RedisScript EXPIRE = RedisScript.of("expire", RedisScript.Reply.INTEGER, PRELUDE + HIDDEN + """
            local maxAge, ph, pl, expired = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6]), 0
            for i = 1, #KEYS - 5 do
              local row, hash = KEYS[5 + i], ARGV[6 + i]
              local f = redis.call('HMGET', row, 's', 'w', 'vh', 'vl')
              local w = tonumber(f[2])
              if f[1] == 'V' and string.sub(hash, 1, 1) ~= '#' and w <= now - maxAge and not hidden(w) then
                local h, l = assign(tonumber(f[3]), tonumber(f[4]), ph, pl)
                redis.call('HSET', row, 's', 'E', 'vh', num(h), 'vl', num(l), 'w', num(now))
                redis.call('HDEL', row, 'v')
                redis.call('ZREM', KEYS[1], hash)
                redis.call('ZADD', KEYS[2], num(now), hash)
                announce(hash, h, l, 'E')
                expired = expired + 1
              end
            end
            return expired
            """);

    // Deletes the given rows still due, silently: values beyond the maximum size (counting those not hidden), none
    // younger than the minimum age. ARGV[4]: the maximum size, ARGV[5]: the minimum age in microseconds, ARGV[5 + i]:
    // the hash of KEYS[5 + i]. Returns the number deleted
    static final RedisScript PRUNE_TO_SIZE = RedisScript.of("pruneToSize", RedisScript.Reply.INTEGER,
            PRELUDE + HIDDEN + """
            local minAge, deleted = tonumber(ARGV[5]), 0
            local excess = redis.call('ZCOUNT', KEYS[1], visible, '+inf') - tonumber(ARGV[4])
            for i = 1, #KEYS - 5 do
              if excess <= 0 then break end
              local row, hash = KEYS[5 + i], ARGV[5 + i]
              local f = redis.call('HMGET', row, 's', 'w')
              local w = tonumber(f[2])
              if f[1] == 'V' and string.sub(hash, 1, 1) ~= '#' and w <= now - minAge and not hidden(w) then
                redis.call('DEL', row)
                redis.call('ZREM', KEYS[1], hash)
                excess = excess - 1
                deleted = deleted + 1
              end
            end
            return deleted
            """);

    // Deletes the given rows still due, silently: any the control row hides (no grace period), and tombstones and
    // expired rows at least the retention old. ARGV[4]: the retention in microseconds, ARGV[4 + i]: the hash of
    // KEYS[5 + i]. Returns the number deleted
    static final RedisScript PRUNE_TOMBSTONES = RedisScript.of("pruneTombstones", RedisScript.Reply.INTEGER,
            PRELUDE + HIDDEN + """
            local retention, deleted = tonumber(ARGV[4]), 0
            for i = 1, #KEYS - 5 do
              local row, hash = KEYS[5 + i], ARGV[4 + i]
              local f = redis.call('HMGET', row, 's', 'w')
              local w = tonumber(f[2])
              if f[1] and string.sub(hash, 1, 1) ~= '#' and (hidden(w) or (f[1] ~= 'V' and w <= now - retention)) then
                redis.call('DEL', row)
                redis.call('ZREM', KEYS[1], hash)
                redis.call('ZREM', KEYS[2], hash)
                deleted = deleted + 1
              end
            end
            return deleted
            """);

    // The stream's position now, from which a subscription reads: {the last entry's ID ("0-0" for none), its number
    // (the counter, 0 for none)}, taken together so that the next entry is the number after it. KEYS: the stream,
    // its counter
    static final RedisScript POSITION = RedisScript.of("position", RedisScript.Reply.ARRAY, """
            local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
            local id = '0-0'
            if #last > 0 then id = last[1][1] end
            return {id, tonumber(redis.call('GET', KEYS[2]) or '0')}
            """);

    // Whether the server has the stream commands the store uses, as the store uses them (in a script, with MINID): a
    // server speaking the protocol may lack some while reporting a Redis version that has them (Dragonfly before 1.4
    // has no XREAD, before 1.3 no MINID, and reports Redis 6.2.11; Garnet runs no scripts on its defaults, and has no
    // streams). Reads first, so that a failure leaves no key behind; the probe's stream is deleted again
    static final RedisScript PROBE = RedisScript.of("probe", RedisScript.Reply.INTEGER, """
            redis.call('XREAD', 'COUNT', 1, 'STREAMS', KEYS[1], '0')
            redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
            redis.call('XADD', KEYS[1], 'MINID', '~', '0', '*', 'probe', '1')
            redis.call('DEL', KEYS[1])
            return 1
            """);

    // Sets the schema marker where there is none and returns the one there. KEYS[1]: the marker, ARGV[1]: this
    // version's
    static final RedisScript MARKER = RedisScript.of("marker", RedisScript.Reply.BULK, """
            local marker = redis.call('GET', KEYS[1])
            if marker then return marker end
            redis.call('SET', KEYS[1], ARGV[1])
            return ARGV[1]
            """);
}

-- Copyright (c) 2009-2013, Scott Vokes <vokes.s@gmail.com>
--
-- All rights reserved.
-- 
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions
-- are met:
--     * Redistributions of source code must retain the above copyright
--       notice, this list of conditions and the following disclaimer.
--     * Redistributions in binary form must reproduce the above
--       copyright notice, this list of conditions and the following
--       disclaimer in the documentation and/or other materials
--       provided with the distribution.
--     * Neither the name of Scott Vokes nor the names of other
--       contributors may be used to endorse or promote products
--       derived from this software without specific prior written
--       permission.
-- 
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
-- FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
-- COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
-- INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
-- BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
-- LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
-- CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
-- LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
-- ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
-- POSSIBILITY OF SUCH DAMAGE.

-- imports
local fmt, concat, pop = string.format, table.concat, table.remove
local assert, error, math, pcall, setmetatable, tonumber, tostring =
    assert, error, math, pcall, setmetatable, tonumber, tostring
    
module(...)
    
local DEFAULT_BRANCH_FACTOR = 16
DEBUG = false

-- invariants:
-- . content is only ever appended at level 0
-- . max level only ever increases 1 at a time
-- . 'trunk' is max-level spine of structure
-- . only trunk branches upward, and only at last node

-- TODO:
-- The 'L' (limb) / 'D' (data) markers should be unnecessary if
-- trunk nodes without upward branches are given an explcit end marker,
-- such as a "00000000000000000000 0\n" link.

-- All jumpropes should belong to a common set, store callbacks etc. there.
JumpropeSet = {}
JumpropeSet.__index = JumpropeSet

-- An individual jumprope handle, inside a JumpropeSet.
Jumprope = {}
Jumprope.__index = Jumprope

-- Sentinel
local UNKNOWN = {}

local function log(...)
    if DEBUG then print(string.format(...)) end
end

-- Make a new jumprope set. Requires several callbacks:
-- GET: (hash -> data | nil, "error")
-- PUT: (hash, data -> true | nil, "error")
-- EXISTS: (hash -> true | false | nil, "error")
-- HASH: (data -> hash)
--
-- BRANCH_FACTOR: 1:N chance in branching.
function init(t)
    local jrs = setmetatable({}, JumpropeSet)
    jrs.get = assert(t.get, "Must specify 'get' callback")
    jrs.exists = assert(t.exists, "Must specify 'exists' callback")
    jrs.put = assert(t.put, "Must specify 'put' callback")
    jrs.hash = assert(t.hash, "Must specify 'hash' callback")
    jrs.hash_len = t.hash_len or jrs.hash("foo"):len()
    jrs.branch_factor = t.branch_factor or DEFAULT_BRANCH_FACTOR
    jrs.min_len = jrs.branch_factor / 4
    jrs.max_len = jrs.branch_factor * 4
    
    jrs._cache = setmetatable({}, {__mode="kv"})
    return jrs
end

-- Get the head hash of a jumprope, or UNKNOWN if not available.
function Jumprope:head()
    return self._headhash
end

-- Hash a data string.
function Jumprope:hash(data)
    return self._set.hash(data)
end

-- Is a hash evenly divisible by the branch factor?
function Jumprope:is_div(hash)
    return self._set:is_div(hash)
end

-- For a limb node string S, return an iterator of (hash, type, length) tuples.
-- Each line should be have the format of e.g.
--     "da4b9237bacccdf19c0760cab7aec4a8359010b0 D 1\n".
-- The hash should be lowercase.
local function iter_hashes(s)
    assert(s, "no string")
    return s:gmatch("(%x+) ([LD]) (%d+)\n")
end

-- Get the total size of the jumprope's data (by summing the trunk's nodes).
function Jumprope:size()
    if self._headhash == UNKNOWN then return nil, "incomplete" end
    if self._size then return self._size end
    
    local t, get = 0, self._set.get
    
    -- get trunk
    local ok, res = pcall(get, self._headhash)
    if not ok then error(res, 0) end   
    
    local hash_iter = iter_hashes(res)
    
    for hash, type, chunk_sz in hash_iter do
        t = t + chunk_sz
    end
    
    self._size = t
    return t
end

-- Get the count of data nodes used in building the jumprope.
-- (mainly used for testing / benchmarking)
function Jumprope:count()
    return self._count
end

local function push(t, v) t[#t+1] = v end


--------------
-- Creation --
--------------

-- Initialize a new jumprope structure (to be built from streamed data).
function JumpropeSet:new()
    local jr = setmetatable({}, Jumprope)
    jr._set = self
    jr._count = 0                -- node count
    jr._headhash = UNKNOWN       -- hash for head node
    jr._limb = {}                -- current limb
    jr._limb_size = 0            -- data bytes within current limb
    jr._stack = {}               -- stack of limbs
    jr._level = 0                -- current level
    jr._max_level = 0            -- max level of trunk
    return jr
end

function JumpropeSet:is_div(hash_str)
    local num = tonumber(hash_str:sub(self.hash_len - 2), 16)
    return num % self.branch_factor == 0
end

local function make_new_limb(self)
    self._limb = {}
    self._limb_size = 0
end

-- Grow successive downward limbs until back at level 0.
local function descend_to_zero(self)
    while self._level > 0 do
        log("growing downward to zero, @ %d", self._level)
        self._level = self._level - 1
        push(self._stack, {self._limb, self._limb_size})
        make_new_limb(self)
    end
end

-- Branch trunk up one level, saving current context, to be completed with
-- the hash of the rest of the jumprope.
local function branch_trunk_upward(self)
    push(self._stack, {self._limb, self._limb_size})
    self._max_level = self._max_level + 1
    self._level = self._level + 1
    make_new_limb(self)
    log("branch_upward to level %d, %d / %d",
        self._level, self._level, self._max_level)
    
    descend_to_zero(self)
    return true
end

-- Append a "hash type length\n" line to the current limb.
-- Type is either "L" (metadata limb) or "D" (data).
local function append_hash(self, type, data, h, limb_len)
    local limb = self._limb
    h = h or self:hash(data)
    local len = (type == "L" and limb_len or data:len())
    assert(len, "no limb length provided")
    push(limb, fmt("%s %s %d\n", h, type, len))
    self._count = self._count + 1
    --print("append_hash: adding ", len, " now ", self._limb_size + len)
    self._limb_size = self._limb_size + len
    log("append_hash %s, type %s, len %d", h, type, #limb)
end

-- Should the current addition also be a breaking point for the current limb?
local function should_break(self, limb, hash, bf)
    local len = #limb
    local div = self:is_div(hash, bf)
    local sb = len >= self._set.max_len or (len >= self._set.min_len and div)
    log("%d, %s -> %s", len, tostring(div), tostring(sb))
    return sb
end

-- Terminate the current limb, popping back up one or more limb(s)
-- according to the hashes of the terminated limbs, then grow back
-- down to limb 0.
local function terminate_branch(self)
    local cur_limb = concat(self._limb)
    local cur_limb_size = self._limb_size
    
    local h = self:hash(cur_limb)
    local cfg = self._set
    local put = cfg.put
    --print("Cls", cur_limb_size)
    local ok, err = pcall(put, h, cur_limb)
    if not ok then return nil, err end
    
    local pair = pop(self._stack)
    self._limb, self._limb_size = pair[1], pair[2]
    assert(self._limb_size)
    --print("Adding", cur_limb_size, " now ", self._limb_size + cur_limb_size)
    log("terminate_branch, level == %d / %d", self._level, self._max_level)
    assert(self._level < self._max_level)
    self._level = self._level + 1
    assert(self._limb_size)
    log("LIMB SIZE", self._limb_size)
    append_hash(self, "L", cur_limb, h, cur_limb_size) --self._limb_size)
    
    local is_trunk = self._level == self._max_level
    if should_break(self, self._limb, h, cfg.branch_factor) then
        log("-- breaking at %d, %s", self._level, tostring(is_trunk))
        if is_trunk then
            branch_trunk_upward(self)
        else
            terminate_branch(self)
        end
    else
        descend_to_zero(self)
    end
    assert(self._level == 0, "should end terminate_branch with level of 0")
    return true
end

-- Sink data into the jumprope, return true | nil, "error".
function Jumprope:sink(data)
    assert(data)
    local h = self:hash(data)
    local cfg = self._set
    local put = cfg.put
    local ok, err = pcall(put, h, data)
    if not ok then return nil, err end
    
    assert(self._level == 0, "Appending data at non-zero level")
    assert(self._limb)
    assert(self._limb_size)
    append_hash(self, "D", data, h)
    
    log("sink %d / %d, %d",
        self._level, self._max_level, self._count)
    
    if should_break(self, self._limb, h, cfg.branch_factor) then
        local is_trunk = self._level == self._max_level
        if is_trunk then   -- trunk; push and increase trunk level
            return branch_trunk_upward(self)
        else               -- branch; close branch and pop to previous
            return terminate_branch(self)
        end
    end
    
    return true
end

-- Close out the current limb.
local function pop_limb(self, put)
    assert(#self._stack > 0)
    local cur_limb = concat(self._limb)
    local pair = pop(self._stack)
    local cur_limb_size = self._limb_size
    self._limb, self._limb_size = pair[1], pair[2]
    --print("pop: adding ", cur_limb_size, " now ", self._limb_size + cur_limb_size)
    self._limb_size = self._limb_size + cur_limb_size
    assert(self._limb_size)
    local h = self:hash(cur_limb)
    local ok, err = pcall(put, h, cur_limb)
    if not ok then return nil, err end
    self._count = self._count + 1
    push(self._limb, fmt("%s L %d\n", h, cur_limb_size))
    return true
end

-- EOF has been reached, close out the intermediate data structures
-- and return the head hash or nil, "error".
function Jumprope:finish()
    local put = self._set.put
    while #self._stack > 0 do
        local ok, err = pop_limb(self, put)
        if not ok then return nil, err end
    end
    local root = concat(self._limb)
    
    local trunk = {}
    local total_size = 0
    for hash, type, len_str in iter_hashes(root) do
        len = tonumber(len_str)
        push(trunk,  {hash, type, len})
        total_size = total_size + len
        --print("TRUNK", hash, type, len)
    end
    
    -- It should have at least one node.
    if root == "" then
        local h = self:hash("")
        local ok, err = pcall(put, h, "")
        if not ok then return nil, err end
        self._count = 1
        root = fmt("%s D 0\n", h)
    end
    local head = self:hash(root)
    local ok, err = pcall(put, head, root)
    if not ok then return nil, err end
    
    -- Clear temporary data
    self._limb = nil
    self._stack = nil
    
    -- Save info about root of structure
    self._headhash = head
    self._size = total_size
    return head
end


---------------
-- Retrieval --
---------------

-- Create a handle to an existing jumprope with the head HEADHASH.
function JumpropeSet:open(headhash)
    assert(headhash, "no hash given")
    local jr = setmetatable({}, Jumprope)
    jr._headhash = headhash
    jr._set = self
    return jr
end

-- Do sanity checks, then get the portion of data[from:to] that falls
-- within from < s < to (zero-indexed).
-- CHUNK is data[offset:offset + chunk_sz].
-- (This is only exported for testing.)
function within_span(chunk, offset, from, to, chunk_sz)
    local of, ot = from - offset, to - offset
    if of < 1 then of = 0 end
    
    assert(offset + chunk_sz >= from, "offset + chunk_sz <= from")
    assert(offset < to, "offset >= to")
    
    local span = ot - of
    local from, to = of + 1, of + span
    if to == math.huge then to = nil end
    return chunk:sub(from, to)
end

-- Get an iterator for the jumprope's data between the 
-- byte offsets FROM < b < TO, which default to 0 and data:len().
-- Since the range ends may not coincide with a chunk boundary,
-- fetch and return subsets of chunks as necessary.
-- 
-- Unlike Lua, this is 0-indexed, i.e., ("blah"):stream(0,2) yields "bl".
function Jumprope:stream(from, to)
    from = from or 0
    to = to or math.huge
    if self._headhash == UNKNOWN then
        error("jumprope is not yet readable", 0)
    end
    local actual_get, cache = self._set.get, self._set._cache
    local get = function(hash)
                    local v = cache[hash]
                    if v then return v end
                    v = actual_get(hash)
                    -- FIXME: disable cache for now, it's
                    -- not being collected properly.
                    --cache[hash] = v
                    return v
                end
    local ok, res = pcall(get, self._headhash)
    if not ok then error(res, 0) end
    
    local offset, stack, hash_iter = 0, {}, iter_hashes(res)
    
    local iterator
    iterator = function()
       if not stack then return nil end  -- already DONE
       local hash, type, chunk_sz = hash_iter()
       local chunk
       
       if hash then                      -- got a chunk
           local post = offset + chunk_sz
           
           -- print(string.format("* %s (%s), %d bytes, offset %d (%s - %s)",
           --                   hash, type, chunk_sz, offset, from, to))
           if post < from then            -- skip chunk
               offset = offset + chunk_sz
               return iterator()
           elseif offset >= to then       -- done with iteration
               stack = nil
               return
           elseif type == "L" then        -- push stack and descend
               assert(offset < to or (offset <= from and post > from))
               push(stack, hash_iter)
               ok, chunk = pcall(get, hash)
               if not ok then return error(chunk, 0) end
               hash_iter = iter_hashes(chunk)
               return iterator()
           elseif type == "D" then        -- yield some/all of data chunk
               ok, chunk = pcall(get, hash)
               if not ok then error(chunk, 0) end
               
               if offset > from and post < to then     -- full yield
                   log("YIELDING CONTENT: %d", chunk:len())
                   offset = post
                   return chunk
               else                                      -- partial yield
                   local part = within_span(chunk, offset, from, to, chunk_sz)
                   log("YIELDING PARTIAL CONTENT: %d", part:len())
                   offset = post
                   return part
               end
           else
               error("Bad type")
           end
       else
           if #stack == 0 then            -- EOF
               stack = nil
               return nil, "done"
           else                   -- pop limb stack and continue
               hash_iter = pop(stack)
               return iterator()
           end
       end
   end
    
   return iterator
end

require "random"
local jumprope = tangram.jumprope
require "crypto"

local floor = math.floor

module(..., package.seeall)

-- Make a table of counts for each JumpropeSet; weak, so they can be GC'd.
local counts = setmetatable({}, {__mode="v"})

local function sha1(data)
   return crypto.digest("sha1", data)
end

function in_mem_JumpropeSet(bf)
   bf = bf or 64
   local store = {}
   local count_fun
   local function get(hash)
      local v = store[hash]
      --print("GET", hash, v and v:len() or "nil")
      if v then return v else
         error("unknown hash: " .. hash)
      end
   end

   local function put(hash, data)
      assert(data, "no data")
      --print("PUT", hash, data:len())
      if not store[hash] then count_fun(true) end
      store[hash] = data
      return true
  end

   local function exists(hash)
       return store[hash] ~= nil
   end

   local jrs = jumprope.init {get=get, put=put, exists=exists, hash=sha1,
                              branch_factor=bf}
   count_fun = function(n)
                  local cur = (counts[jrs] or 0)
                  if n then counts[jrs] = cur + 1 end
                  return cur
               end
   return jrs, count_fun
end

local concat = table.concat
local char = string.char

function mk_random_string(sz, seed)
   seed = seed or 1
   local r = random.new()
   r:seed(seed)
   local buf = {}
   for i=1,sz do
      buf[i] = char(r:value(256) - 1)
   end
   return concat(buf)
end

function test_two_empty_JRs_should_have_the_same_head_hash()
   local js = in_mem_JumpropeSet()
   local j1, j2 = js:new(), js:new()
   -- Add the empty string to #1 and finish it.
   assert(j1:sink(""))
   assert(j1:finish())

   -- Just finish #2 with it empty.
   assert(j2:finish())

   assert_true(j1:head(), "head hash should exist")
   assert_equal(j1:head(), j2:head(), "head hashes should match")
end

function test_empty_JRs_should_have_one_node()
   local js = in_mem_JumpropeSet()
   local j = js:new()

   assert(j:sink(""))
   assert(j:finish())
   assert_equal(1, j:count())
end

function test_two_JRs_with_the_same_single_string_should_have_the_same_hash()
   local js = in_mem_JumpropeSet()
   local j1, j2 = js:new(), js:new()
   local s = "brevity is the soul of wit"

   assert(j1:sink(s))
   assert(j1:finish())

   assert(j2:sink(s))
   assert(j2:finish())

   assert_true(j1:head(), "should exist")
   assert_equal(j1:head(), j2:head(), "head hashes should match")
end

function test_two_JRs_with_the_same_set_of_strings_should_have_the_same_hash()
   local js = in_mem_JumpropeSet()
   local j1, j2 = js:new(), js:new()
   local s = "brevity is the soul of wit"

   for c in s:gmatch("(.)") do assert(j1:sink(c)) end
   assert(j1:finish())

   for c in s:gmatch("(.)") do assert(j2:sink(c)) end
   assert(j2:finish())

   assert_true(j1:head(), "should exist")
   assert_equal(j1:head(), j2:head(), "head hashes should match")
end

function iter_str(s, chunk_size)
   local i, len = 1, s:len()
   return function ()
             if i > len then return nil end
             local chunk = s:sub(i, i + chunk_size - 1)
             i = i + chunk_size
             return chunk
      end
end

function test_test_two_JRs_with_the_same_large_string_should_have_the_same_hash()
   local js = in_mem_JumpropeSet()
   local j1, j2 = js:new(), js:new()

   -- 1 MB string of random binary data
   local s = mk_random_string(1024 * 1024, 23)

   -- add in 1kb chunks
   for chunk in iter_str(s, 1024) do
      j1:sink(chunk)
      j2:sink(chunk)
   end

   j1:finish()
   j2:finish()

   assert_equal(j1:head(), j2:head())
end

function test_two_JRs_with_the_same_string_should_add_few_new_nodes_when_changed()
   local js, count_fun = in_mem_JumpropeSet()
   local j1, j2 = js:new(), js:new()

   -- 1 MB string of random binary data
   local s = mk_random_string(1024 * 1024, 23)

   -- add in 1kb chunks
   for chunk in iter_str(s, 1024) do
      j1:sink(chunk)
   end
   local ok, err = j1:finish()
   assert(ok, err)

   local pre_count = count_fun()

   local i = 0
   for chunk in iter_str(s, 1024) do
      i = i + 1
      if i == 100 then
         j2:sink(("x"):rep(1024))
      else
         j2:sink(chunk)
      end
   end

   assert(j2:finish())

   local post_count = count_fun()

   assert_not_equal(j1:head(), j2:head(), "head hashes should not match")
   assert_lte(0.01 * pre_count, post_count - pre_count)
end

-- Test that finish -> pop_limb computes limb size correctly
function test_sink_100_one_byte_chunks_and_total_length()
   local js = in_mem_JumpropeSet()
   local j = js:new()
   local lim = 100

   -- add "0" .. "9" over and over
   for i=0,lim - 1 do
      local chunk = tostring(i % 10)
      j:sink(chunk)
   end
   assert(j:finish())

   assert_equal(lim, j:size(), "size should match")
end

-- Test that terminate_branch computes limb size correctly
function test_sink_1000_one_byte_chunks_and_total_length()
   local js = in_mem_JumpropeSet()
   local j = js:new()
   local lim = 1000

   -- add "0" .. "9" over and over
   for i=0,lim - 1 do
      local chunk = tostring(i % 10)
      j:sink(chunk)
   end
   assert(j:finish())

   assert_equal(lim, j:size(), "size should match")
end

function test_put_failures_should_be_passed_to_user()
   local count = 5
   local function nop() end
   local function put(hash, data)
      count = count - 1
      if count == 0 then error("fail", 0) end
      return true
   end

   local jrs = jumprope.init({put=put, get=nop, exists=nop, hash=sha1})
   local jr = jrs:new()
   for i=1,5 do
      local ok, err = jr:sink("blah")
      if i == 5 then
         assert_nil(ok, "should fail")
         assert_equal("fail", err, "should get error message")
      else
         assert(ok)
      end
   end 
end

function test_within_span()
   local ws = jumprope.within_span
   local s = "abcdefghijklmnopqrstuvwxyz"
   local function ws(exp, offset, from, to)
      assert_equal(exp, jumprope.within_span(s:sub(offset+1, offset+1+5),
                                             offset, from, to, 5))
   end 

   ws("a", 0, 0, 1)
   ws("b", 0, 1, 2)
   ws("b", 1, 1, 2)
   ws("cdef", 1, 2, 6)
   ws("yz", 24, 24, 26)
   ws("z", 25, 25, 26)
end

-- Compare strings, but in a way that makes off-by-ones obvious, rather than
-- printing "got (VERY LONG STRING), expected (OTHER VERY LONG STRING)".
local function off_by_one_check(rejoined, expected)
   assert_equal(expected:sub(1, 2), rejoined:sub(1, 2), "first 2 chars should match")
   assert_equal(expected:sub(-2), rejoined:sub(-2), "last 2 chars should match")
   assert_equal(expected:len(), rejoined:len(), "sizes should match")
   assert_true(rejoined == expected, "should equal input")
end

function check_it(s, chunk_sz, from, to)
   local js = in_mem_JumpropeSet()
   local j = js:new()

   -- add in chunk_sz pieces
   for chunk in iter_str(s, chunk_sz) do
      -- print("< ", chunk)
      j:sink(chunk)
   end
   j:finish()
   assert_equal(s:len(), j:size(), "j:size() is incorrect")

   local buf = {}
   local iter = assert(j:stream(from, to))

   for chunk in iter do
     buf[#buf+1] = chunk
     -- print(">", chunk)
   end

   local expected = s:sub(from + 1, to)
   local rejoined = concat(buf)

   off_by_one_check(rejoined, expected)
end

function test_get_content_from_part_of_small_string()
   local s = "abcdefghijklmnopqrstuvwxyz"
   for chunk_sz=2,4 do
      for start=0,25 do
         for len=1,6 do
            --print("\n### CSL ", chunk_sz, start, len)
            check_it(s, chunk_sz, start, start + len)
         end
      end
   end
end

function test_a_jumprope_iterator_should_return_the_same_content_as_the_original_input()
   -- 1 MB string of random binary data
   local s = mk_random_string(1024 * 1024, 23)
   check_it(s, 63, 0, s:len() - 1)
end

function test_get_content_from_halfway_to_the_end()
   -- 1 MB string of random binary data
   local sz = 1024 * 1024
   local s = mk_random_string(sz, 23)
   local start = floor(sz/2)
   check_it(s, 63, start, sz - 1)
end

function test_get_the_first_half_of_the_content()
   -- 1 MB string of random binary data
   local sz = 1024 * 1024
   local s = mk_random_string(sz, 23)
   check_it(s, 63, 0, floor(sz/2))
end

function test_open_existing_jumprope()
   local sz = 1024 * 1024
   local s = mk_random_string(sz, 27)

   local chunk_sz = 999
   local from, to = 0, sz

   local js = in_mem_JumpropeSet()
   local j = js:new()

   -- add in chunk_sz pieces
   for chunk in iter_str(s, chunk_sz) do
      -- print("< ", chunk)
      j:sink(chunk)
   end

   local head = j:finish()
   assert_true(head, "Didn't return headhash")

   local j2 = js:open(head)
   
   local buf = {}

   local iter = assert(j2:stream(from, to))

   for chunk in iter do
     buf[#buf+1] = chunk
   end

   -- check content
   local rejoined = table.concat(buf)
   off_by_one_check(rejoined, s)

   -- check size
   assert_equal(j:size(), j2:size(), "Size doesn't match")
end

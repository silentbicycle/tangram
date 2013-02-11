require "hashchop"
require "crypto"
require "lfs"

require "tangram.db"
local jumprope = require "tangram.jumprope"

-- Use zlib for compression, if Lua wrapper is available.
local compress, decompress
local ok, zlib = pcall(require, "zlib")

if ok and zlib then
    compress = zlib.compress
    decompress = zlib.decompress
end

module(..., package.seeall)

--local function log(...) print(...) end
local function log(...) end
local function printf(...) io.write(string.format(...)) end

local usage = {}

local function print_usage(cmdname)
    for _,row in ipairs(usage[cmdname]) do print(row) end
    os.exit(1)
end

local function file_exists(path)
    return lfs.attributes(path) ~= nil
end

local function mkdir_if_nonexistent(path)
    if not file_exists(path) then
        assert(lfs.mkdir(path))
    end
end

local function sha1(data)
    return crypto.digest("sha1", data)
end

local function db_path(cfg)
    return cfg.base_path .. "db.sql"
end

local function pop(t) return table.remove(t, 1) end

-- Create callbacks jumprope expects for disk I/O
local function init_callbacks(cfg)
    local store_path = cfg.base_path .. "store"
    
    local function hash_fn(hash)
        local base = store_path
        local head, rest = hash:match("(%w%w%w)(%w+)")
        local fullpath = table.concat{base, "/", head, "/", rest}
        local basedir = table.concat{base, "/", head}
        return fullpath, basedir, rest
    end

    local function get(hash)
        assert(hash, "no hash given")
        local path = hash_fn(hash)
        local f = assert(io.open(path, "r"))
        local data = f:read("*a")
        if decompress then data = decompress(data) end
        log("GET ", hash, data:len())
        f:close()
        return data
    end

    local function exists(hash)
        assert(hash, "no hash given")
        local path = hash_fn(hash)
        return file_exists(path)
    end

    local function put(hash, content)
        if cfg.dry_run then return end
        assert(hash, "no hash given")
        log("SAVE ", hash, " => ", content:len())
        local path, basedir, rest = hash_fn(hash)
        if not file_exists(basedir) then
            assert(lfs.mkdir(basedir))
        end
        if file_exists(path) then return end
        local f = assert(io.open(path, "w"))
        if compress then content = compress(content) end
        f:write(content)
        f:close()
    end

    return {get=get, put=put, exists=exists}
end

usage["init"] = {
    "Usage for 'init' command:",
    "init [-b RH_BITS] [-f BRANCH_FACTOR]",
    "  RH_BITS: Bits for rolling hash bitmask (average chunk size ~ 2^RH_BITS).",
    "  BRANCH_FACTOR: Each Jumprope limb has a 1:BF chance of terminating.",
}

function cmd_init(arg, cfg)
    local path = cfg.base_path or DEFAULTS.base_path
    local store_path = path .. "store"

    mkdir_if_nonexistent(path)
    mkdir_if_nonexistent(store_path)

    local opts = {
        path = db_path(cfg),
        rh_bits = nil,
        branch_factor = nil,
    }

    while true do
        if arg[1] == '-b' then  -- rh bits
            pop(arg)
            local b = pop(arg)
            b = assert(tonumber(b), "Invalid spec for RH bits")
            opts.rh_bits = b
        elseif arg[1] == '-f' then
            pop(arg)
            local f = pop(arg)
            f = assert(tonumber(f), "Invalid spec for branch factor")
            opts.branch_factor = f
        elseif arg[1] then
            print_usage("init")
        else
            break
        end
    end

    local _ = assert(tangram.db.init_db(opts))
    printf("Initialized jumprope store in: %s\n", opts.path)
end

local function add_mainloop(f, hc, jr, read_size)
    local size = 0

    -- Read the input, sink it into the hashchopper, and for every
    -- complete chunk it yields, sink it into the jumprope.
    -- This is pretty simple, but error handling adds a bit of code.
    while true do
        local rd = f:read(read_size)
        if rd == nil then break end -- EOF
        local res = hc:sink(rd)
        if res == "ok" then
            -- happy case: bump acc'd size and continue
            size = size + rd:len()
            log("SUNK: ", rd:len())
        elseif res == "overflow" then
            error("Chunk size too large for hashchopper")
        elseif res == "full" then
            error("Buffer full, needs more flushing")
        else
            error("Unexpected: " .. tostring(res))
        end

        while true do
            local chunk, err = hc:poll()
            if chunk then
                log("POLL: ", chunk:len())
                -- Since the jumprope's callbacks are blocking,
                -- we can just bail out on error here.
                assert(jr:sink(chunk))
            elseif err == "underflow" then     -- no more chunks
                break
            elseif err == "overflow" then
                error("Too large to fit in buffer")
            else
                error("Unexpected: " .. tostring(res))
            end 
        end
    end
    
    local rem, err = hc:finish()
    if rem then
        -- sink the remaining content
        log("REM: ", rem:len())
        assert(jr:sink(rem))
    elseif err == "overflow" then
        error("Too large to fit in buffer")
    end
    return size
end

usage["add"] = {
    "Usage for 'add' command:",
    "add [-n SAVE-AS-NAME] [FILENAME or -]",
}

function cmd_add(arg, cfg)
    local fname = "-"
    local save_as = nil

    if arg[1] == '-n' then
        pop(arg)
        save_as = pop(arg)
        if not save_as then print_usage("add") end
    end
    if arg[1] then fname = arg[1] end
    local f = io.stdin

    if fname == "-" then
        fname = "<stdin>"
    else
        f = assert(io.open(fname, "r"))
    end
    local db = assert(tangram.db.open(db_path(cfg)))

    local db_cfg = db:get_config()
    cfg.bits = assert(db_cfg.rh_bits)
    cfg.branch_factor = assert(db_cfg.branch_factor)

    local cbs = init_callbacks(cfg)
    local hc = hashchop.new(cfg.bits)
    local jrs = assert(jumprope.init{get=cbs.get, put=cbs.put,
                                     exists=cbs.exists, hash=sha1})
    local jr = jrs:new()
    local size = add_mainloop(f, hc, jr, 2^cfg.bits)

    -- Terminate jumprope and save file metadata
    local headhash = assert(jr:finish())

    if cfg.dry_run then
        printf("Not saving (dry run), head hash %s\n", headhash)
        return
    end
    local id = assert(db:add_file(headhash, save_as or fname, size))
    
    if id == 0 then
        printf("File is already stored, head hash %s\n", headhash)
    else
        printf("Added file %d, head hash %s\n", id, headhash)
    end
end

usage["get"] = {
    "Usage for 'get' command:",
    "get [-f | -h] KEY [OUT_FILE]",
    "  If no out file path is provided, it will print to stdout.",
    "  If neither '-f' (file ID) nor '-h' (hash) is used, it will",
    "  attempt to guess whether the key is a file ID or hash.",
    -- "[-r FROM:TO] "
    -- "  -r can be used to fetch only a specific byte-range of the file.",
}

local function get_headhash_from_args(arg, cfg, db)
    -- arg[1] => hash? file ID? filename?
    local arg_type = "unknown"

    local v
    if arg[1] == '-f' then
        arg_type = "id"         -- file ID
        pop(arg)
        v = pop(arg)
    elseif arg[1] == '-h' then
        arg_type = "hash"       -- hash hex digest
        pop(arg)
        v = pop(arg)
    else            -- does the arg looks like a file ID or hash?
        v = pop(arg)
        if v == nil then print_usage("get") end
        v = tostring(v)
        if v:match("^[0-9]+$") then
            arg_type = "id"
        elseif v:match("^[0-9a-fA-F]+$") then
            arg_type = "hash"
        else
            print_usage("get")
        end
    end

    if arg_type == "hash" then
        local hashes, conflicts = {}, {}
        for hash in db:get_hash_completions(v) do
            hashes[#hashes+1] = hash.hash
        end
        if #hashes == 0 then
            return nil, conflicts
        elseif #hashes > 1 then
            return nil, hashes
        else
            return hashes[1], nil
        end
    elseif arg_type == "id" then
        local id = tonumber(v)
        local info = db:get_file_info(id)
        if info and info.hash then
            return info.hash, {}
        else
            printf("Bad file ID: %d\n", id)
            os.exit(1)
        end
    end
end

function cmd_get(arg, cfg)
    local db = assert(tangram.db.open(db_path(cfg)))

    -- Get a single headhash or nil and a "did you mean X,Y,Z..." list.
    local headhash, conflicts = get_headhash_from_args(arg, cfg, db)

    if not headhash then
        if #conflicts == 0 then
            printf("No completion found for hash prefix\n")
            os.exit(1)
        end

        printf("Ambiguous jumprope spec:\n")
        for _,h in ipairs(conflicts) do
            printf("  %s\n", h)
        end
        os.exit(1)
    end

    local f = io.stdout
    if arg[1] then
        f = assert(io.open(arg[1], "w"))
    end
    local cbs = init_callbacks(cfg)
    local jrs = assert(jumprope.init{get=cbs.get, put=cbs.put,
                                     exists=cbs.exists, hash=sha1})
    local jr = jrs:open(headhash)

    for chunk in jr:stream() do
        f:write(chunk)
    end
    f:close()
end

function cmd_list(arg, cfg)
    local db = assert(tangram.db.open(db_path(cfg)))

    printf("%-4s  %-10s  %-19s  %-10s  %s\n",
           "ID", "hash", "time (UTC)", "size", "filename")
    for row in db:get_files() do
        printf('%-4d  %s  %s  %-10d  %s\n',
               row.id, row.hash:sub(1,10), row.timestamp,
               row.size, row.name)
    end
end

function cmd_test(arg, cfg)
    local ok = pcall(require, "lunatest")
    if not ok then
        print("test command requires lunatest.")
        os.exit(1)
    end
    
    require "tangram.test_db"
    require "tangram.test_jumprope"

    lunatest.suite("tangram.test_db")
    lunatest.suite("tangram.test_jumprope")
    lunatest.run()
end

function cmd_info(arg, cfg)
    -- info for file ID: get tags
    local db = assert(tangram.db.open(db_path(cfg)))
    local id = assert(pop(arg), "Not a valid file ID")
    id = assert(tonumber(id), "Not a valid file ID")

    local info = assert(db:get_file_info(id))
    for _,key in ipairs{"id", "hash", "timestamp", "size", "name"} do
        if info[key] then printf("%s %s\n", key, info[key]) end
    end

    local props = db:get_properties(id)
    for k,v in pairs(props) do
        printf("  %s%s%s\n", k, v ~= "" and " - " or "", v)
    end
end

function cmd_forget(arg, cfg)
    local db = assert(tangram.db.open(db_path(cfg)))
    local id = assert(tonumber(pop(arg)), "Not a valid file ID")
    assert(db:rm_file(id))
end

function cmd_gc(arg, cfg)
    -- do mark/sweep GC on the file content, delete any node not touched

    -- local marks = {}
    -- for headhash in files:
    --     fetch all non-chunk nodes & save hashes in marks[]
    -- for hash in chunks_on_disk:
    --     if not marks[hash]:
    --         rm(file(hash))
    error "TODO"
end

usage["prop"] = {
    "Usage for 'prop' command:",
    "  prop add ID KEY [VALUE] -- set file ID's property KEY to VALUE (or \"\").",
    "  prop del ID -- delete all properties for file ID.",
    "  prop del ID KEY -- delete property KEY for file ID.",
}

function cmd_prop(arg, cfg)
    local db = assert(tangram.db.open(db_path(cfg)))
    local mode = pop(arg)
    if mode == "add" then    -- prop add ID KEY VAL
        local id = assert(tonumber(pop(arg)), "Not a valid file ID")
        local key = assert(pop(arg), "Missing property key")
        local value = pop(arg) or ""
        assert(db:add_property(id, key, value))
    elseif mode == "del" then  -- prop del ID KEY
        local id = assert(tonumber(pop(arg)), "Not a valid file ID")
        local key = pop(arg)
        if key then
            assert(db:rm_property(id, key))
        else
            assert(db:rm_property(id))
        end
    else
        print_usage("prop")
    end
end

usage["search"] = {
    "Usage for 'search' command:",
    "  search name PATTERN -- search for files whose name matches PATTERN.",
    "  search prop KEY -- search for files who have property KEY.",
    "  search prop KEY VALUE -- search for files who have VALUE for property KEY.",
}

function cmd_search(arg, cfg)
    local db = assert(tangram.db.open(db_path(cfg)))
    local mode = pop(arg)
    if mode == "name" then  -- search name PATTERN
        local pattern = assert(pop(arg), "Missing name search pattern")
        for row in db:search_name(pattern) do
            printf("%d  %s\n", row.id, row.name)
        end
    elseif mode == "prop" then -- search prop KEY [VALUE]
        local key = assert(pop(arg), "Missing property search key")
        local value = pop(arg)
        for row in db:search_property(key, value) do
            printf("%d  %s  %s  %s\n", row.id, row.name, row.key, row.value)
        end
    else
        print_usage("search")
    end
end

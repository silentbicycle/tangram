-- This is an odd one, because it actually returns a table called "sqlite3".
require "lsqlite3"

require "tangram.defaults"

local sqlite3 = sqlite3
local assert, print, setmetatable = assert, print, setmetatable
local defaults = DEFAULTS

module(...)

local schema = [[
-- known files
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,      -- head hash
    name TEXT NOT NULL,      -- filename
    timestamp TIME NOT NULL, -- creation datetime()
    size INTEGER NOT NULL,   -- file size
    CONSTRAINT duped_file UNIQUE (hash, name) ON CONFLICT IGNORE
);

-- key value store for arbitrary file metadata
CREATE TABLE IF NOT EXISTS properties (
    fid INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (fid) REFERENCES file(id)
);

CREATE INDEX IF NOT EXISTS prop_index ON properties (fid, key);

-- configuration for server
CREATE TABLE IF NOT EXISTS config (
    version TEXT NOT NULL,            -- internal data format version
    rh_bits INTEGER NOT NULL,         -- bits for rolling hash
    branch_factor INTEGER NOT NULL    -- branch factor for jumprope
);
]]

-- file info DB
DB = {}
DB.__index = DB

function open(path)
    path = path or ":memory:"
    local db, err = sqlite3.open(path)
    if not db then
        return nil, ("Unable to open tangram store DB at %s"):format(path)
    end
    local res = {_db=db, _cache={}}
    return setmetatable(res, DB)
end

function DB:stmt(sql)
    local db, cache = self._db, self._cache
    if cache[sql] then
        local stmt = cache[sql]
        stmt:reset()
        return stmt
    else
        local stmt, err = db:prepare(sql)
        if not stmt then
            assert(nil, db:error_message())
        end
        cache[sql] = stmt
        return stmt
    end
end

function DB:last_insert_rowid() return self._db:last_insert_rowid() end
function DB:errmsg() return self._db:errmsg() end

function DB:add_file(hash, name, size)
    local stmt = self:stmt([[
INSERT INTO files (hash, name, size, timestamp) VALUES (?, ?, ?, datetime());]])
    stmt:bind_values(hash, name, size)
    local res, err = stmt:step()
    if res == sqlite3.DONE then
        return self:last_insert_rowid()
    else
        return nil, self:errmsg()
    end
end

function DB:rm_file(id)
    local stmt = self:stmt("DELETE FROM files WHERE id == ?;")
    stmt:bind_values(id)
    local res, err = stmt:step()
    if res == sqlite3.DONE then
        return self:rm_property(id)
    else
        return nil, db:errmsg()
    end
end

-- Get an iterator for all files.
function DB:get_files()
    local stmt = self:stmt("SELECT * FROM files;")
    return stmt:nrows()
end

-- Get array of hashes starting with HASH.
function DB:get_hash_completions(hash)
    local stmt = self:stmt("SELECT hash FROM files WHERE hash LIKE ?;")
    stmt:bind_values((hash or "") .. "%")
    return stmt:nrows()
end

function DB:add_property(id, key, value)
    local stmt = self:stmt([[
INSERT INTO properties (fid, key, value) VALUES (?, ?, ?);]])
    stmt:bind_values(id, key, value)
    local res, err = stmt:step()
    if res == sqlite3.DONE then
        return self:last_insert_rowid()
    else
        return nil, self:errmsg()
    end
end

-- Get info for a single file ID.
function DB:get_file_info(id)
    local stmt = self:stmt("SELECT * FROM files WHERE id == ?;")
    stmt:bind_values(id)
    local info = {}
    for row in stmt:nrows() do
        return row
    end
    return nil, "not found"
end

-- Get a table of properties associated with a file ID.
function DB:get_properties(id)
    local stmt = self:stmt("SELECT key, value FROM properties WHERE fid == ?;")
    stmt:bind_values(id)
    local props = {}
    for row in stmt:nrows() do
        props[row.key] = row.value
    end
    return props
end

function DB:rm_property(id, key)
    local stmt
    if key then
        stmt = self:stmt([[
DELETE FROM properties
WHERE fid == ? AND key == ?;]])
        stmt:bind_values(id, key)
    else
        stmt = self:stmt("DELETE FROM properties WHERE fid == ?;")
        stmt:bind_values(id)
    end

    local res, err = stmt:step()
    if res == sqlite3.DONE then
        return self:last_insert_rowid()
    else
        return nil, self:errmsg()
    end
end

function DB:search_name(name)
    local stmt = self:stmt("SELECT id, name FROM files WHERE name LIKE ?;")
    stmt:bind_values("%" .. name .. "%")
    return stmt:nrows()
end

function DB:search_hash(hash)
    local stmt = self:stmt("SELECT id FROM files WHERE hash LIKE ?;")
    stmt:bind_values(hash .. "%")
    return stmt:nrows()
end

-- Search by key and/or value.
function DB:search_property(key, value)
    local stmt
    if key and value then
        stmt = self:stmt([[
SELECT f.id, f.name, p.key, p.value FROM files f, properties p
WHERE p.key == ? AND p.value == ? AND f.id == p.fid;]])
        stmt:bind_values(key, value)
    else
        stmt = self:stmt([[
SELECT f.id, f.name, p.key, p.value FROM files f, properties p
WHERE p.key == ? AND f.id == p.fid;]])
        stmt:bind_values(key)
    end
    return stmt:nrows()
end

function DB:get_config()
    local stmt = self:stmt("SELECT * FROM config;")
    for row in stmt:nrows() do
        return row
    end
end

function init_db(opts)
    opts = opts or {}
    opts.rh_bits = opts.rh_bits or defaults.rh_bits
    opts.branch_factor = opts.branch_factor or defaults.branch_factor

    local path = opts.path or ":memory:"
    local sql_db, err = sqlite3.open(path)
    if not sql_db then
        return nil, "Failed to create database at " .. path
    end
    local code = sql_db:exec(schema)
    if code ~= sqlite3.OK then
        return nil, sql_db:error_message()
    end

    local db = setmetatable({_db=sql_db, _cache={}}, DB)

    local stmt = db:stmt([[
INSERT INTO config (version, rh_bits, branch_factor)
VALUES (?, ?, ?);]])
    stmt:bind_values(defaults.version, opts.rh_bits, opts.branch_factor)

    local res, err = stmt:step()
    if res == sqlite3.DONE then
        return db
    else
        return nil, sql_db:errmsg()
    end
end

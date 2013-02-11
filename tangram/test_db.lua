local db = tangram.db

module(..., package.seeall)

function test_db_creation()
    assert_true(db.init_db())
end

local exhash = "970318968feb640da723b8826861e41f0718a487"

local function def_db()
    return assert(db.init_db())
end

function test_db_and_file_and_check()
    local db = def_db()
    local res, err = db:add_file(exhash, "bananas.txt", 23)
    assert_equal(1, res)
    local found
    for f in db:get_files() do
        if f.id == 1 then found = true end
    end
    assert_true(found)
end

function test_db_add_and_remove()
    local db = def_db()
    local res, err = db:add_file(exhash, "bananas.txt", 23)
    assert_equal(1, res)
    res, err = db:rm_file(1)
    assert_true(res)
    for f in db:get_files() do
        if f.id == 1 then fail("not deleted") end
    end
end

function test_db_hash_completions()
    local db = def_db()
    db:add_file(exhash, "bananas.txt", 23)
    local hashes = {}
    for h in db:get_hash_completions(exhash:sub(1,4)) do
        hashes[#hashes+1] = h.hash
    end
    assert_equal(exhash, hashes[1])
    local hash_h = "ffff" .. exhash:sub(5, exhash:len())
    local hash_t = exhash:sub(1, exhash:len() - 4) .. "ffff"
    db:add_file(hash_h, "head.txt", 10)
    db:add_file(hash_t, "tail.txt", 20)
    
    hashes = {}
    for h in db:get_hash_completions(exhash:sub(1,4)) do
        hashes[#hashes+1] = h.hash
    end
    assert_equal(2, #hashes)
    table.sort(hashes)
    assert_equal(exhash, hashes[1])
    assert_equal(hash_t, hashes[2])
end

function test_add_property()
    local db = def_db()
    local id, err = db:add_file(exhash, "bananas.txt", 23)
    db:add_property(id, "version", "1")
    local props = db:get_properties(id)
    assert_equal("1", props.version)
end

function test_add_and_rm_property()
    local db = def_db()
    local id, err = db:add_file(exhash, "bananas.txt", 23)
    db:add_property(id, "version", "1")
    db:rm_property(id)
    local props = db:get_properties(id)
    assert_equal(nil, props.version)
end

function test_search_name()
    local db = def_db()
    local id, err = db:add_file(exhash, "bananas.txt", 23)

    for row in db:search_name("bananas") do
        if row.id == id then return end
    end
    fail("not found")
end

function test_search_hash()
    local db = def_db()
    local id, err = db:add_file(exhash, "bananas.txt", 23)

    for row in db:search_hash(exhash) do
        if row.id == id then return end
    end
    fail("not found")
end

function test_search_property()
    local db = def_db()
    local id, err = db:add_file(exhash, "bananas.txt", 23)
    db:add_property(id, "version", "1")

    for row in db:search_property("version") do
        if row.id == id then return end
    end
    fail("not found")
end

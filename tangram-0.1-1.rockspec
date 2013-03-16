package = "tangram"
version = "0.1-1"
source = {
    url = "git://github.com/silentbicycle/tangram.git",
    tag = "v0.1-1",
    file = "tangram-0.1-1.tar.gz",
    dir = "tangram",
}
description = {
    summary    = "A Jumprope-based content store",
    detailed   = [[
This is a standalone content store, somewhat like the .git directory
that git uses for internal storage. However, while git is best suited to
storing versioned collections of relatively small, diff-able files,
tangram is best at storing large files.
            
It is based on the Jumprope, a data structure I invented. The Jumprope
is a kind of tree of arrays of data chunks, whose overall shape is
derived from the data itself -- duplicated sections of files coalesce
together to branches that are automatically shared, and identical files
end up with the same overall identifier.
]],
license    = "BSD",
homepage   = "github.com/silentbicycle/tangram/",
maintainer = "Scott Vokes (vokes.s@gmail.com)",
}
dependencies = {
    "lua >= 5.1",
    "hashchop >= 0.8-0",
    "slncrypto >= 1.1-1",
    "lzlib >= 0.3-3",
    "luafilesystem >= 1.6.2-1",
    "lsqlite3 >= 0.8-1",
    "lunatest >= 0.9.1-1",
    "lrandom >= 20101118-1",
}
build = {
    type = "none",
    install = {
        bin = { ["tangram"] = "tangram.lua"},
        lua = {
            ['tangram.cmds'] = "tangram/cmds.lua",
            ['tangram.db'] = "tangram/db.lua",
            ['tangram.defaults'] = "tangram/defaults.lua",
            ['tangram.init'] = "tangram/init.lua",
            ['tangram.jumprope'] = "tangram/jumprope.lua",
            ['tangram.main'] = "tangram/main.lua",
            ['tangram.test_db'] = "tangram/test_db.lua",
            ['tangram.test_jumprope'] = "tangram/test_jumprope.lua",
        },
    }
}

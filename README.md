tangram: a Jumprope-based content store.


# Overview

This is a standalone content store, somewhat like the .git directory
that git uses for internal storage. However, while git is best suited to
storing versioned collections of relatively small, diff-able files,
tangram is best at storing large files.

It is based on the Jumprope, a data structure I invented. The Jumprope
is a kind of tree of arrays of data chunks, whose overall shape is
derived from the data itself -- duplicated sections of files coalesce
together to branches that are automatically shared, and identical files
end up with the same overall identifier.

(This is a central component of scatterbrain, a distributed filesystem
I've been working on, but also useful on its own. Since Jumpropes use
[content-addressable storage][CAS] and all data is immutable, it doesn't
really matter where the data is located -- scatterbrain mirrors the data
over a somewhat Dynamo-like distributed hash table, which periodically
checks that all live content is mirrored in a sufficient number of
nodes. I'm still working on the network logic, though, and it will be a
separate project)

[CAS]: http://en.wikipedia.org/wiki/Content-addressable_storage


## Example Use Cases

 * Storing many variants of genetic data
 * Storing design / multimedia assets
 * Backing up lots of incremental virtual machine snapshots


## Features

 * Automatic de-duplication of content
 * Automatic detection of identical files
 * A tagging / property system for saving and searching by file metadata
 * High throughput (e.g. HD video pipes to mplayer w/ out skips)


# License

This is released under a 3-clause BSD license. Be nice.


# Current Status

The system works, but the command-line interface and installation
process are still evolving, and a bit rough around the edges.
(Thanks, early adopters. Constructive feedback is appreciated.)

I have tested it on Linux, OpenBSD, and OSX.

I *haven't* tested it on Windows yet, but it shouldn't take major
effort to port - there isn't anything OS-specific besides the process
to create a native Lua extension and some default paths.


# Installation

The installation process should eventually be replaced by
`brew install tangram`, `apt-get install tangram`, `pkg_add tangram`,
and the like, but it's still pretty manual.


## Dependencies

 All Lua dependencies are available via [LuaRocks](http://luarocks.org).

 * Lua (http://lua.org)
 * SQLite3 (http://sqlite.org)
 * A C compiler
 * libhashchop and its Lua wrapper (http://github.com/silentbicycle/hashchop/)
 * luafilesystem
 * luacrypto (for SHA1 hashing)
 * zlib and its lua wrapper
 * SQLite3's lua wrapper
 * lunatest (for testing)


## How to Install

 * Install [Lua](http://lua.org). 
 * Install [SQLite3](http://sqlite.org), if you don't have like
   a dozen copies of it lying around already.
 * Install [LuaRocks](http://luarocks.org), the de facto standard packaging
   system for Lua. (If you don't want to use LuaRocks, install the other
   Lua dependencies yourself.)
 * Use LuaRocks to install the `crypto`, `zlib`, `lfs`, `sqlite3`,
   and `lunatest` packages. Type e.g. `luarocks install crypto`.
 * Download [libhashchop](https://github.com/silentbicycle/hashchop),
   build it, and then build and install the lua wrapper with `make lua`
   and `make lua-install`. Or, if you want to do it by hand, copy the
   dynamic library to wherever Lua puts its native extensions on your
   system. (To figure this out, you can fire up the Lua REPL and type
   `=package.cpath`. On Unix-like OSs, it's typically something like
   `/usr/local/lib/lua/5.1/`.)
 * Copy the `tangram` subdirectory into Lua's package path (typically
   "/usr/local/share/lua/5.1/", check `package.path`), so that the
   tangram.* packages can be loaded.
 * Copy the tangram.lua script into your path somewhere.


## Example usage

    $ tangram.lua init            # create a content store w/ default settings
    $ tangram.lua add foo.bar     # add a file to the store
    $ cmd | tangram.lua add -     # add to the store from stdin
    $ tangram.lua list            # list known files
    $ tangram.lua get 1           # get file with ID #1, print to stdout
    $ tangram.lua get 1 foo.baz   # get file with ID #1, save to foo.baz


# Options

All commands take the following arguments (which should appear *before*
the command name):

 * -d: dry run, don't write to disk
 * -v: verbose
 * -s PATH: use custom store path instead of default


# Commands

## help: print help message

Print help.

## version: print version

Print the version.

## init: initialize data store

Initialize a data store.

    tangram init [-b BITS] [-f BRANCH_FACTOR]

Arguments:

 * -b BITS - Set number of bits for rolling hash bitmask (determines chunk size)
 * -f BF - Set branching factor (determines average Jumprope limb length)

## get: get a file

Get file content from the store.

    tangram get [-f | -h] KEY [OUT_FILE]
    
-f or -h specify that the key is a file ID (-f) or hash (-h), otherwise
it will try to infer the right thing. If OUT_FILE is given, it will save
the content to that file, otherwise it will print to stdout.

## add: add a file

Add a file to the store.

    tangram add [-n NAME] [FILENAME or -]

Arguments:

 * -n NAME - Store input file as NAME.

## info: get info

Print metadata about a file.

    tangram info ID
    
TODO: the info command (without an ID) should print info about the store config

## list: list known files

Print basic info about all stored files.

## prop: get/set property

Get / set a property on a file. These properties don't have any internal
meaning, but exist as a hook to track content metadata.

    tangram prop add ID KEY
    tangram prop add ID KEY VALUE
    tangram prop del ID
    tangram prop del ID KEY

## search: search by name or property

Search by name or property.

    tangram search prop KEYNAME
    tangram search prop KEYNAME VALUENAME
    tangram search name PATTERN

## forget: stop tracking a file

Stop tracking a file. To actually remove content from the store, use the
GC command (not yet implemented).

    tangram forget ID

## test: run tests

Run unit tests. (Requires lunatest.)


# Future Developments

 * Better documentation of the Jumprope data structure. Its reference
   implementation is included, and (IMHO) commented well, but there are
   some subtleties. In the mean time, [my StrangeLoop talk][talk]
   includes an attempt to convey my intuitions about how it works.

[talk]: www.infoq.com/presentations/Data-Structures

 * Retrieving specific byte-ranges of content. The Jumprope library
   supports it, but it isn't part of the CLI yet.

 * While there is currently no interface for it, the Jumprope has the
   necessary metadata to accelerate diff-ing of very large files.
   (It automatically identifies large subsets of the files that are
   known to be identical and can be skipped.)
   
 * Forgetting a file removes it from the file index, but does not delete
   data files that are no longer in use. I haven't gotten around to
   implementing the necessary garbage collection yet.

 * There isn't any attempt to take advantage of the Jumprope's
   embarassingly parallelizable retrieval. Scatterbrain uses async IO to
   spread reads over the network, and to maintain an arbitrarily large
   look-ahead buffer for the streaming data, but this doesn't bother
   with that: it would would only complicate things and lead to more
   disk contention. There may be advantages in taking advantage of
   parallelism by different means, though.


# Acknowledgements

Thanks to everyone who has given me feedback along the way, particularly
Mike English and Jessica Kerr.

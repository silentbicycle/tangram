-- Copyright (c) 2012-2013, Scott Vokes <vokes.s@gmail.com>
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

module(..., package.seeall)

local usage

-- global switches
local switches = {
    ['-v'] = {l="verbose", f=function(a,c) c.verbose = true end },
    ['-d'] = {l="dry_run", f=function(a,c) c.dry_run = true end },
    ['-s'] = {l="store path", f=function(a,c) c.base_path = table.remove(a, 1) end },
}

cmds = {
    ['help'] = {l="print this message", f=function(a,c) usage() end },
    ['init'] = {l="initialize data store", f=tangram.cmds.cmd_init,
                o="-r RH_BITS -b BRANCH_FACTOR"},
    ['version'] = {l="print version",
                   f=function(a,c) print(DEFAULTS.version); os.exit(0) end },
    ['add'] = {l="add a file", f=tangram.cmds.cmd_add, o="PATH"},
    ['get'] = {l="get a file", f=tangram.cmds.cmd_get,
               o="-r RANGE NAME"},
    ['list'] = {l="list known files", f=tangram.cmds.cmd_list },
    ['test'] = {l="run tests", f=tangram.cmds.cmd_test },
    ['info'] = {l="get info", f=tangram.cmds.cmd_info, o="ID"},
    ['forget'] = {l="forget a file", f=tangram.cmds.cmd_forget, o="ID"},
    ['prop'] = {l="get/set property", f=tangram.cmds.cmd_prop},
    ['search'] = {l="search", f=tangram.cmds.cmd_search},
    --['gc'] = {l="collect garbage", f=tangram.cmds.cmd_gc },
}

function usage()
    local b = {}
    local A = function(...) b[#b+1] = string.format(...) end
    A("tangram: jumprope-based archiver by %s\n", DEFAULTS.author)
    A("    version %s\n", DEFAULTS.version)
    A("Usage: \n")
    A("  Arguments\n")
    for k,v in pairs(switches) do
        A("    %s: %s\n", k, v.l)
    end
    A("  Commands\n")
    for k,v in pairs(cmds) do
        A("    %s: %s\n", k, v.l)
    end
    io.write(table.concat(b))
    os.exit(0)
end

local function proc_args(arg)
    local cfg = {}

    cfg.base_path = os.getenv("TANGRAM_PATH")

    while true do
        local a = table.remove(arg, 1)
        if not a then break end
        if cmds[a] then cfg.cmd = cmds[a]; break end
        local sf = switches[a]
        if not sf then print("Bad arg: ", a); usage() end
        sf.f(arg, cfg)
    end

    cfg.bits = cfg.bits or DEFAULTS.rh_bits
    cfg.base_path = cfg.base_path or DEFAULTS.base_path

    -- Ensure trailing "/" for base path.
    if cfg.base_path:sub(-1) ~= "/" then
        cfg.base_path = cfg.base_path .. "/"
    end

    return cfg
end

function main(arg)
    if #arg <= 0 then
        usage()
    else
        local cfg = proc_args(arg)
        if cfg.cmd then
            cfg.cmd.f(arg, cfg)
        end
    end
end

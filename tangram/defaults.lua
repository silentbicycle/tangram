local HOME = assert(os.getenv("HOME"))

DEFAULTS = {
    author = "Scott Vokes <vokes.s@gmail.com>",
    version = "0.01.02",

    -- base path for local content store
    base_path = HOME .. "/.tangram/",

    -- bitmask size for rolling hash
    rh_bits = 15,

    -- branching factor for jumprope
    branch_factor = 16,
}

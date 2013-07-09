-- Make `now` available to everything so we don't have to pass it around.
local now          = tonumber(table.remove(ARGV, 2))
local now          = assert(
    now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

-- Top-level table that holds "constructors" for our classes.
local Plines = {}


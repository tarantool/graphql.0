--- Various utility function used across the graphql module sources and tests.

local log = require('log')

local utils = {}

--- Log an error and the corresponding backtrace in case of the `func` function
--- call raises the error.
function utils.show_trace(func, ...)
    local args = {...}
    return select(2, xpcall(
        function() return func(unpack(args)) end,
        function(err)
            log.info('ERROR: ' .. tostring(err))
            log.info(debug.traceback())
        end
    ))
end

--- Recursively checks whether `sub` fields values are match `t` ones.
function utils.is_subtable(t, sub)
    for k, v in pairs(sub) do
        if type(v) == 'table' then
            if t[k] == nil then
                return false
            end
            if not utils.is_subtable(t[k], v) then
                return false
            end
        elseif t[k] ~= v then
            return false
        end
    end
    return true
end

--- Check whether table is an array.
---
--- Based on [that][1] implementation.
--- [1]: https://github.com/mpx/lua-cjson/blob/db122676/lua/cjson/util.lua
---
--- @tparam table table to check
--- @return[1] `true` if passed table is an array (includes the empty table
--- case)
--- @return[2] `false` otherwise
function utils.is_array(table)
    if type(table) ~= 'table' then
        return false
    end
    local max = 0
    local count = 0
    for k, _ in pairs(table) do
        if type(k) == 'number' then
            if k > max then
                max = k
            end
            count = count + 1
        else
            return false
        end
    end
    if max > count * 2 then
        return false
    end

    return max >= 0
end

return utils

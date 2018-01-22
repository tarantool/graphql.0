local log = require('log')

local utils = {}

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
--- Based on [that][1].
--- [1]: https://github.com/mpx/lua-cjson/blob/db122676/lua/cjson/util.lua
--- @param table
--- @return True if table is an array
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

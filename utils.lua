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

return utils

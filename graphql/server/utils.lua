local fio = require('fio')

local utils = {}

function utils.file_exists(name)
    return fio.stat(name) ~= nil
end

function utils.read_file(path)
    local file = fio.open(path)
    if file == nil then
        return nil
    end
    local buf = {}
    while true do
        local val = file:read(1024)
        if val == nil then
            return nil
        elseif val == '' then
            break
        end
        table.insert(buf, val)
    end
    file:close()
    return table.concat(buf, '')
end

function utils.script_path()
   local str = debug.getinfo(2, "S").source:sub(2)
   return str:match("(.*/)") or '.'
end

return utils

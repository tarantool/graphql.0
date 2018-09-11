local log = require('log')
local json = require('json')

local storage = {}
local storage_functions = {}

storage_functions.batch_select = function(space_name, index_name, keys, opts)
    log.info(('batch_select(%s, %s, %s, %s)'):format(
        space_name, index_name or 'nil', json.encode(keys),
        json.encode(opts)))

    local index = index_name == nil and
        box.space[space_name].index[0] or
        box.space[space_name].index[index_name]
    local results = {}

    for _, key in ipairs(keys) do
        local tuples = index:select(key, opts)
        table.insert(results, tuples)
    end

    return results
end

storage_functions.space_call = function(space_name, index_name, method_name,
        key, options)
    local space = box.space[space_name]
    assert(space, space_name)
    local index = index_name and space.index[index_name] or space
    assert(index, index_name)
    local method = index[method_name]
    assert(method, method_name)
    return method(index, key, options)
end

function storage.functions()
    return table.copy(storage_functions)
end

function storage.init()
    for k, v in pairs(storage_functions) do
        rawset(_G, k, v)
    end
end

return storage

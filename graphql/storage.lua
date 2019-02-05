--- Functions to be exposed from shard storage nodes.

local log = require('log')
local json = require('json')
-- local yaml = require('yaml')

local storage = {}

local function batch_select(space_name, index_name, keys, opts)
    log.info(('batch_select(%s, %s, %s, %s)'):format(
        space_name, index_name or 'nil', json.encode(keys),
        json.encode(opts)))

    local index = index_name == nil and
        box.space[space_name].index[0] or
        box.space[space_name].index[index_name]
    local results = {}

    for _, key in ipairs(keys) do
        -- log.info('batch_select key:\n' .. json.encode(key))
        local tuples = index:select(key, opts)
        -- log.info('batch_select tuples:\n' .. yaml.encode(tuples))
        table.insert(results, tuples)
    end

    -- log.info('batch_select result:\n' .. yaml.encode(results))
    return results
end

function storage.functions()
    return {
        batch_select = batch_select,
    }
end

function storage.init()
    for k, v in pairs(storage.functions()) do
        _G[k] = v
    end
end

-- declare globals for require('strict').on()
for k, _ in pairs(storage.functions()) do
    _G[k] = nil
end

return storage

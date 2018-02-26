--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as space accessor and provides the
--- `accessor_space.new` function to create a new space data accessor instance.

local accessor_general = require('graphql.accessor_general')

local accessor_space = {}

-- Check whether a collection (it is space for that accessor) exists.
local function is_collection_exists(collection_name)
    return box.space[collection_name] ~= nil
end

--- Get index to perform `:pairs({v1, ...})`.
--- @return index or nil
local function get_index(collection_name, index_name)
    return box.space[collection_name].index[index_name]
end

--- Get primary index to perform `:pairs()` (fullscan).
local function get_primary_index(collection_name)
    return box.space[collection_name].index[0]
end

--- Convert a tuple to an object.
---
--- @tparam string collection_name
--- @tparam cdata/table tuple
--- @tparam function default unflatten action, call it in the following way:
---
--- ```
--- return default(collection_name, tuple)
--- ```
local function unflatten_tuple(collection_name, tuple, default)
    return default(collection_name, tuple)
end

--- Create a new space data accessor instance.
function accessor_space.new(opts, funcs)
    local funcs = funcs or {}
    assert(type(funcs) == 'table',
        'funcs must be nil or a table, got ' .. type(funcs))

    for k, v in pairs(funcs) do
        assert(type(k) == 'string',
            'funcs keys must be strings, got ' .. type(k))
        assert(type(v) == 'function',
            'funcs values must be functions, got ' .. type(v))
    end

    local res_funcs = {
        is_collection_exists = funcs.is_collection_exists or
            is_collection_exists,
        get_index = funcs.get_index or get_index,
        get_primary_index = funcs.get_primary_index or get_primary_index,
        unflatten_tuple = funcs.unflatten_tuple or unflatten_tuple,
    }

    return accessor_general.new(opts, res_funcs)
end

return accessor_space

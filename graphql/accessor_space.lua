--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as space accessor and provides the
--- `accessor_space.new` function to create a new space data accessor instance.

local utils = require('graphql.utils')
local accessor_general = require('graphql.accessor_general')

local check = utils.check

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
--- @tparam table opts
--- * `use_tomap` (boolean, default: false; whether objects in collection
---   collection_name intended to be unflattened using
---   `tuple:tomap({names_only = true})` method instead of
---   `compiled_avro_schema.unflatten(tuple)`
--- @tparam function default unflatten action, call it in the following way:
---
---     return default(collection_name, tuple, opts)
---
local function unflatten_tuple(collection_name, tuple, opts, default)
    local opts = opts or {}
    check(opts, 'opts', 'table')
    if opts.use_tomap then
        return tuple:tomap({ names_only = true })
    end
    return default(collection_name, tuple, opts)
end

--- Convert an object to a tuple.
---
--- @tparam string collection_name
--- @tparam table obj
--- @tparam table opts
--- * `service_fields_defaults` (list (Lua table), default: empty list; list of
---   values to set service fields)
--- @tparam function default flatten action, call it in the following way:
---
---    return default(collection_name, obj, opts)
---
--- @treturn cdata/table `tuple`
local function flatten_object(collection_name, obj, opts, default)
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local service_fields_defaults = opts.service_fields_defaults or {}
    check(service_fields_defaults, 'service_fields_defaults', 'table')
    return default(collection_name, obj, opts)
end

--- Generate update statements for tarantool from xflatten input.
---
--- @tparam string collection_name
--- @tparam table xobject xflatten input
--- @tparam table opts
--- * `service_fields_defaults` (list (Lua table), default: empty list; list of
---   values to set service fields)
--- @tparam function default xflatten action, call it in the following way:
---
---    return default(collection_name, xobject, opts)
---
--- @treturn cdata/table `tuple`
local function xflatten(collection_name, xobject, opts, default)
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local service_fields_defaults = opts.service_fields_defaults or {}
    check(service_fields_defaults, 'service_fields_defaults', 'table')
    return default(collection_name, xobject, opts)
end

--- Insert a tuple into a collection.
---
--- @tparam string collection_name
---
--- @tparam cdata/table tuple
---
--- @treturn cdata/table `tuple`
local function insert_tuple(collection_name, tuple)
    return box.space[collection_name]:insert(tuple)
end

--- Update a tuple with an update statements.
---
--- @tparam string collection_name
---
--- @param key primary key
---
--- @tparam table statements
---
--- @tparam table opts
---
--- * tuple (ignored in accessor_space)
---
--- @treturn cdata/table `tuple`
local function update_tuple(collection_name, key, statements, opts)
    local opts = opts or {}
    check(opts, 'opts', 'table')
    return box.space[collection_name]:update(key, statements)
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
        flatten_object = funcs.flatten_object or flatten_object,
        xflatten = funcs.xflatten or xflatten,
        insert_tuple = funcs.insert_tuple or insert_tuple,
        update_tuple = funcs.update_tuple or update_tuple,
    }

    return accessor_general.new(opts, res_funcs)
end

return accessor_space

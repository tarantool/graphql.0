--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as space accessor and provides the
--- `accessor_space.new` function to create a new space data accessor instance.

local utils = require('graphql.utils')
local accessor_general = require('graphql.accessor_general')

local check = utils.check

local accessor_space = {}

--- Check whether a collection (it is space for that accessor) exists.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @treturn boolean
local function is_collection_exists(self, collection_name)
    check(self, 'self', 'table')
    return box.space[collection_name] ~= nil
end

--- Get index to perform `:pairs({v1, ...})`.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @tparam string index_name
---
--- @return index or nil
local function get_index(self, collection_name, index_name)
    check(self, 'self', 'table')
    return box.space[collection_name].index[index_name]
end

--- Get primary index to perform `:pairs()` (fullscan).
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @return index or nil
local function get_primary_index(self, collection_name)
    check(self, 'self', 'table')
    return box.space[collection_name].index[0]
end

--- Convert a tuple to an object.
---
--- @tparam table self accessor_general instance
--- @tparam string collection_name
--- @tparam cdata/table tuple
--- @tparam table opts
--- * `use_tomap` (boolean, default: false; whether objects in collection
---   collection_name intended to be unflattened using
---   `tuple:tomap({names_only = true})` method instead of
---   `compiled_avro_schema.unflatten(tuple)`
--- @tparam function default unflatten action, call it in the following way:
---
---     return default(self, collection_name, tuple, opts)
---
local function unflatten_tuple(self, collection_name, tuple, opts, default)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    if opts.use_tomap then
        return tuple:tomap({ names_only = true })
    end
    return default(self, collection_name, tuple, opts)
end

--- Convert an object to a tuple.
---
--- @tparam table self accessor_general instance
--- @tparam string collection_name
--- @tparam table obj
--- @tparam table opts
--- * `service_fields_defaults` (list (Lua table), default: empty list; list of
---   values to set service fields)
--- @tparam function default flatten action, call it in the following way:
---
---    return default(self, collection_name, obj, opts)
---
--- @treturn cdata/table `tuple`
local function flatten_object(self, collection_name, obj, opts, default)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local service_fields_defaults = opts.service_fields_defaults or {}
    check(service_fields_defaults, 'service_fields_defaults', 'table')
    return default(self, collection_name, obj, opts)
end

--- Generate update statements for tarantool from xflatten input.
---
--- @tparam table self accessor_general instance
--- @tparam string collection_name
--- @tparam table xobject xflatten input
--- @tparam table opts
--- * `service_fields_defaults` (list (Lua table), default: empty list; list of
---   values to set service fields)
--- @tparam function default xflatten action, call it in the following way:
---
---    return default(self, collection_name, xobject, opts)
---
--- @treturn cdata/table `tuple`
local function xflatten(self, collection_name, xobject, opts, default)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local service_fields_defaults = opts.service_fields_defaults or {}
    check(service_fields_defaults, 'service_fields_defaults', 'table')
    return default(self, collection_name, xobject, opts)
end

--- Insert a tuple into a collection.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @tparam cdata/table tuple
---
--- @treturn cdata/table `tuple`
local function insert_tuple(self, collection_name, tuple)
    check(self, 'self', 'table')
    return box.space[collection_name]:insert(tuple)
end

--- Update a tuple with an update statements.
---
--- @tparam table self accessor_general instance
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
local function update_tuple(self, collection_name, key, statements, opts)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    return box.space[collection_name]:update(key, statements)
end

--- Delete tuple by a primary key.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @param key primary key
---
--- @tparam table opts
---
--- * tuple (ignored in accessor_space)
---
--- @treturn cdata tuple
local function delete_tuple(self, collection_name, key, opts)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    return box.space[collection_name]:delete(key)
end

--- Create a new space data accessor instance.
function accessor_space.new(opts, funcs)
    local funcs = funcs or {}
    check(funcs, 'funcs', 'table')

    for k, v in pairs(funcs) do
        check(k, 'funcs key', 'string')
        check(v, 'funcs value', 'function')
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
        delete_tuple = funcs.delete_tuple or delete_tuple,
    }

    return accessor_general.new(opts, res_funcs)
end

return accessor_space

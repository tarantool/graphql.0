--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as shard accessor and provides the
--- `accessor_shard.new` function to create a new shard data accessor instance.

local json = require('json')
local utils = require('graphql.utils')
local shard = utils.optional_require('shard')
local accessor_general = require('graphql.accessor_general')

local check = utils.check

local accessor_shard = {}

local LIMIT = 100000 -- XXX: we need to raise an error when a limit reached

local function shard_check_error(func_name, result, err)
    if result ~= nil then return end
    error(('%s: %s'):format(func_name, json.encode(err)))
end

-- Check whether a collection (it is sharded space for that accessor) exists.
local function is_collection_exists(collection_name)
    local func_name = 'accessor_shard.is_collection_exists'
    local exists
    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            local cur, err = shard:space_call(collection_name, node,
                function(space_obj)
                    return space_obj ~= nil
                end)
            shard_check_error(func_name, cur, err)
            assert(exists == nil or cur == exists,
                ('space "%s" exists on some shards, ' ..
                'but does not on others'):format(collection_name))
            exists = cur
        end
    end
    return exists
end

--- Internal function to use in @{get_index}; it is necessary because
--- determining whether the index exists within a shard cluster is
--- not-so-trivial as for local spaces.
local function is_index_exists(collection_name, index_name)
    local func_name = 'accessor_shard.is_index_exists'
    local exists
    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            local cur, err = shard:space_call(collection_name, node,
                function(space_obj)
                    return space_obj.index[index_name] ~= nil
                end)
            shard_check_error(func_name, cur, err)
            assert(exists == nil or cur == exists,
                ('index "%s" of space "%s" exists on some shards, ' ..
                'but does not on others'):format(index_name, collection_name))
            exists = cur
        end
    end
    return exists
end

--- Get index to perform `:pairs({v1, ...})`.
--- @return index or nil
local function get_index(collection_name, index_name)
    if not is_index_exists(collection_name, index_name) then
        return nil
    end

    local index = setmetatable({}, {
        __index = {
            pairs = function(self, value, opts)
                local func_name = 'accessor_shard.get_index.<index>.pairs'
                local opts = opts or {}
                opts.limit = opts.limit or LIMIT
                local tuples, err = shard:secondary_select(collection_name,
                    index_name, opts, value, 0)
                shard_check_error(func_name, tuples, err)
                local cur = 1
                local function gen()
                    if cur > #tuples then return nil end
                    local res = tuples[cur]
                    cur = cur + 1
                    return cur, res
                end
                return gen, nil, nil
            end
        }
    })
    return index
end

--- Get primary index to perform `:pairs()` (fullscan).
local function get_primary_index(collection_name)
    return get_index(collection_name, 0)
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
---     return default(collection_name, tuple)
---
local function unflatten_tuple(collection_name, tuple, opts, default)
    if opts.use_tomap then
        return tuple:tomap({ names_only = true })
    end

    return default(collection_name, tuple)
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

--- Insert a tuple into a collection.
---
--- @tparam string collection_name
---
--- @tparam cdata/table tuple
---
--- @treturn cdata/table `tuple`
local function insert_tuple(collection_name, tuple)
    local tuples = shard:insert(collection_name, tuple)
    check(tuples, 'tuples', 'table')
    assert(#tuples >= 1, 'expected >= 1 tuples inserted, got ' ..
        tostring(#tuples))
    return tuples[1]
end

--- Create a new shard data accessor instance.
function accessor_shard.new(opts, funcs)
    local funcs = funcs or {}
    assert(type(funcs) == 'table',
        'funcs must be nil or a table, got ' .. type(funcs))
    assert(shard ~= nil,
        'tarantool/shard module is needed to working with accessor_shard')

    for k, v in pairs(funcs) do
        assert(type(k) == 'string',
            'funcs keys must be strings, got ' .. type(k))
        assert(type(v) == 'table',
            'funcs values must be functions, got ' .. type(v))
    end

    local res_funcs = {
        is_collection_exists = funcs.is_collection_exists or
            is_collection_exists,
        get_index = funcs.get_index or get_index,
        get_primary_index = funcs.get_primary_index or get_primary_index,
        unflatten_tuple = funcs.unflatten_tuple or unflatten_tuple,
        flatten_object = funcs.flatten_object or flatten_object,
        insert_tuple = funcs.insert_tuple or insert_tuple,
    }

    return accessor_general.new(opts, res_funcs)
end

return accessor_shard

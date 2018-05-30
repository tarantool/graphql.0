--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as shard accessor and provides the
--- `accessor_shard.new` function to create a new shard data accessor instance.

local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')
local shard = utils.optional_require('shard')
local accessor_general = require('graphql.accessor_general')

local check = utils.check

local accessor_shard = {}

local LIMIT = 100000 -- XXX: we need to raise an error when a limit reached
-- shard module calculates sharding key by the first field of a tuple
local SHARD_KEY_FIELD_NO = 1

local index_info_cache = {}

local function shard_check_error(func_name, result, err)
    if result ~= nil then return end
    error(('%s: %s'):format(func_name, json.encode(err)))
end

-- Should work for shard-1.2 and shard-2.1 both.
local function shard_check_status(func_name)
    if box.space._shard == nil then return end

    local mode = box.space._shard:get({'RESHARDING_STATE'})
    local resharding_is_in_progress = mode ~= nil and #mode >= 2 and
        type(mode[2]) == 'number' and mode[2] > 0
    if resharding_is_in_progress then
        error(('%s: shard cluster is in the resharding state, ' ..
            'modification requests are temporary forbidden'):format(
            func_name))
    end
end

--- Determines whether certain fields of two tables are the same.
---
--- Table fields of t1 and t2 are compared recursively by all its fields.
---
--- @tparam table t1
---
--- @tparam table t2
---
--- @tparam table fields list of fields like {'a', 'b', 'c'}
---
--- @treturn boolean
local function compare_table_by_fields(t1, t2, fields)
    for _, field_name in ipairs(fields) do
        local v1 = t1[field_name]
        local v2 = t2[field_name]
        if type(v1) ~= type(v2) then
            return false
        elseif type(v1) == 'table' then
            local ok = utils.is_subtable(v1, v2)
            ok = ok and utils.is_subtable(v2, v1)
            if not ok then return false end
        else
            if v1 ~= v2 then return false end
        end
    end
    return true
end

--- Get index object from net_box under the shard module.
---
--- The function performs some optimistic consistency checks and raises an
--- error in the case. It caches results and returns a result from the cache
--- for succeeding calls.
---
--- XXX: Implement some cache clean up strategy and a way to manual cache
--- purge.
---
--- @tparam string collection_name
---
--- @tparam string index_name
---
--- @return index object
local function get_index_info(collection_name, index_name)
    local func_name = 'accessor_shard.get_index_info'
    local index_info

    -- get from the cache if exists
    if index_info_cache[collection_name] ~= nil then
        index_info = index_info_cache[collection_name][index_name]
        if index_info ~= nil then
            return index_info
        end
    end

    local fields_to_compare = {'unique', 'parts', 'id', 'type', 'name'}

    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            local result, err = shard:space_call(collection_name, node,
                function(space_obj)
                    return space_obj.index[index_name]
                end)
            shard_check_error(func_name, result, err)
            if index_info == nil then
                index_info = result
            end
            local ok = compare_table_by_fields(index_info, result,
                fields_to_compare)
            assert(ok, ('index %s of space "%s" is different between ' ..
                'nodes:\n%s\n%s'):format(json.encode(index_name),
                collection_name, yaml.encode(index_info), yaml.encode(result)))
        end
    end

    -- write to the cache
    if index_info_cache[collection_name] == nil then
        index_info_cache[collection_name] = {}
    end
    index_info_cache[collection_name][index_name] = index_info

    return index_info
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

--- Determines major version of the shard module in use.
---
--- @treturn number 1 or 2
local function major_shard_version()
    return type(shard.wait_for_shards_to_go_online) == 'function' and 2 or 1
end

--- Get tuple by a primary key.
---
--- @tparam string collection_name
---
--- @param key primary key
---
--- @treturn cdata tuple
local function get_tuple(collection_name, key)
    local func_name = 'accessor_shard.get_tuple'
    local index = get_index(collection_name, 0)
    local tuples = {}
    for _, t in index:pairs(key, {limit = 2}) do
        table.insert(tuples, t)
    end
    assert(#tuples ~= 0,
        ('%s: expected one tuple by the primary key %s, got 0'):format(
        func_name, json.encode(key)))
    assert(#tuples == 1,
        ('%s: expected one tuple by the primary key %s, got more then one')
        :format(func_name, json.encode(key)))
    return tuples[1]
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
    local func_name = 'accessor_shard.insert_tuple'

    shard_check_status(func_name)

    local result, err = shard:insert(collection_name, tuple)
    shard_check_error(func_name, result, err)

    if major_shard_version() == 2 then
        -- result is the inserted tuple
        return result
    else
        -- result is the table of inserted tuples (one for each node)
        check(result, 'result', 'table')
        assert(#result >= 1, 'expected >= 1 tuples inserted, got ' ..
            tostring(#result))
        return result[1]
    end
end

--- Perform a space operation on a shard cluster either on master or on all
--- nodes of a relica set depending on a shard version and a shard
--- configuration.
---
--- @tparam string collection_name
---
--- @tparam table nodes a replica set
---
--- @tparam string operation space operation
---
--- @return a result of the operation (the result for the master node)
local function space_operation(collection_name, nodes, operation, ...)
    local func_name = 'accessor_shard.space_operation'
    -- The `only_master` flag is whether built-in tarantool replication is used
    -- to transfer modifications to slave servers in a replica set. If so we
    -- should perform an operation only on the master node.
    --
    -- Note: shard-3* ignores the replication option and lean on the build-in
    -- tarantool replication unconditionally.
    --
    -- Note: master node is the first one within `nodes`.
    local only_master = major_shard_version() == 2 or
        shard.pool.configuration.replication
    local master_result
    for _, node in ipairs(nodes) do
        local result, err = shard:single_call(collection_name, node, operation,
            ...)
        shard_check_error(func_name, result, err)
        if master_result == nil then
            master_result = result
        end
        if only_master then
            break
        end
    end
    return master_result
end

--- Delete tuple by a primary key.
---
--- @tparam string collection_name
---
--- @param key primary key
---
--- @tparam table opts
---
--- * tuple (cdata/table, optional); the same as in @{update_tuple}
---
--- @treturn cdata tuple
local function delete_tuple(collection_name, key, opts)
    local func_name = 'accessor_shard.delete_tuple'

    local opts = opts or {}
    check(opts, 'opts', 'table')
    check(opts.tuple, 'opts.tuple', 'nil', 'cdata', 'table')

    shard_check_status(func_name)

    local tuple = opts.tuple or get_tuple(collection_name, key)
    local nodes = shard.shard(tuple[SHARD_KEY_FIELD_NO])
    local tuple = space_operation(collection_name, nodes, 'delete', key)
    assert(tuple ~= nil,
        ('cannot find a tuple in collection "%s" by the primary key %s ' ..
        'to delete'):format(collection_name, json.encode(key)))
    return tuple
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
--- * tuple (cdata/table, optional); a user can provide the original tuple to
---   save one broadcast request performing to determine shard key / needed
---   replica set
---
--- @treturn cdata/table `tuple`
local function update_tuple(collection_name, key, statements, opts)
    local func_name = 'accessor_shard.update_tuple'

    local opts = opts or {}
    check(opts, 'opts', 'table')
    check(opts.tuple, 'opts.tuple', 'nil', 'cdata', 'table')

    shard_check_status(func_name)

    local is_shard_key_to_be_updated = false
    for _, statement in ipairs(statements) do
        -- statement is {operator, field_no, value}
        local field_no = statement[2]
        if field_no == SHARD_KEY_FIELD_NO then
            is_shard_key_to_be_updated = true
            break
        end
    end

    -- We follow tarantool convention and disallow update of primary key parts.
    local primary_index_info = get_index_info(collection_name, 0)
    for _, statement in ipairs(statements) do
        -- statement is {operator, field_no, value}
        local field_no = statement[2]
        for _, part in ipairs(primary_index_info.parts) do
            -- We generate the same message as tarantool to easier testing.
            assert(field_no ~= part.fieldno, ("Attempt to modify a tuple " ..
                "field which is part of index '%s' in space '%s'"):format(
                tostring(primary_index_info.name), collection_name))
        end
    end

    if is_shard_key_to_be_updated then
        local tuple = delete_tuple(collection_name, key, {tuple = opts.tuple})
        tuple = tuple:update(statements)
        return insert_tuple(collection_name, tuple)
    else
        local tuple = opts.tuple or get_tuple(collection_name, key)
        local nodes = shard.shard(tuple[SHARD_KEY_FIELD_NO])
        local tuple = space_operation(collection_name, nodes, 'update', key,
            statements)
        assert(tuple ~= nil,
            ('cannot find a tuple in collection "%s" by the primary key %s ' ..
            'to update'):format(collection_name, json.encode(key)))
        return tuple
    end
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
        xflatten = funcs.xflatten or xflatten,
        insert_tuple = funcs.insert_tuple or insert_tuple,
        update_tuple = funcs.update_tuple or update_tuple,
    }

    return accessor_general.new(opts, res_funcs)
end

return accessor_shard

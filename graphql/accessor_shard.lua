--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as shard accessor and provides the
--- `accessor_shard.new` function to create a new shard data accessor instance.

local json = require('json')
local digest = require('digest')
local utils = require('graphql.utils')
local shard = utils.optional_require('shard')
local accessor_general = require('graphql.accessor_general')
local accessor_shard_helpers = require('graphql.accessor_shard_helpers')
local accessor_shard_index_info = require('graphql.accessor_shard_index_info')
local accessor_shard_cache = require('graphql.accessor_shard_cache')

local check = utils.check

local accessor_shard = {}

local LIMIT = 100000 -- XXX: we need to raise an error when a limit reached
-- shard module calculates sharding key by the first field of a tuple
local SHARD_KEY_FIELD_NO = 1

-- {{{ helpers

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
            accessor_shard_helpers.shard_check_error(func_name, cur, err)
            assert(exists == nil or cur == exists,
                ('index "%s" of space "%s" exists on some shards, ' ..
                'but does not on others'):format(index_name, collection_name))
            exists = cur
        end
    end
    return exists
end

--- Determines major version of the shard module in use.
---
--- @treturn number 1 or 2
local function major_shard_version()
    return type(shard.wait_for_shards_to_go_online) == 'function' and 2 or 1
end

--- Get tuple by a primary key.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @param key primary key
---
--- @treturn cdata tuple
local function get_tuple(self, collection_name, key)
    local func_name = 'accessor_shard.get_tuple'
    check(self, 'self', 'table')
    local index = self.funcs.get_primary_index(self, collection_name)
    local tuples = {}
    local out = {} -- XXX: count fetched_tuples_cnt in statistics
    for _, t in index:pairs(key, {limit = 2}, out) do
        table.insert(tuples, t)
    end
    check(out.fetched_tuples_cnt, 'out.fetched_tuples_cnt', 'number')
    assert(#tuples ~= 0,
        ('%s: expected one tuple by the primary key %s, got 0'):format(
        func_name, json.encode(key)))
    assert(#tuples == 1,
        ('%s: expected one tuple by the primary key %s, got more then one')
        :format(func_name, json.encode(key)))
    return tuples[1]
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
        accessor_shard_helpers.shard_check_error(func_name, result, err)
        if master_result == nil then
            master_result = result
        end
        if only_master then
            break
        end
    end
    return master_result
end

local function get_shard_key_hash(key)
    local shards_n = #shard.shards
    local num = type(key) == 'number' and key or digest.crc32(key)
    return 1 + digest.guava(num, shards_n)
end

-- }}}

--- Check whether a collection (it is sharded space for that accessor) exists.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @treturn boolean
local function is_collection_exists(self, collection_name)
    local func_name = 'accessor_shard.is_collection_exists'
    check(self, 'self', 'table')
    local exists
    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            local cur, err = shard:space_call(collection_name, node,
                function(space_obj)
                    return space_obj ~= nil
                end)
            accessor_shard_helpers.shard_check_error(func_name, cur, err)
            assert(exists == nil or cur == exists,
                ('space "%s" exists on some shards, ' ..
                'but does not on others'):format(collection_name))
            exists = cur
        end
    end
    return exists
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
    if not is_index_exists(collection_name, index_name) then
        return nil
    end

    -- XXX: wrap all data into the table, don't create the capture
    local index = setmetatable({}, {
        __index = {
            pairs = function(_, value, opts, out)
                local func_name = 'accessor_shard.get_index.<index>.pairs'

                -- perform select
                local opts = opts or {}
                opts.limit = opts.limit or LIMIT
                local tuples, err = shard:secondary_select(collection_name,
                    index_name, opts, value, 0)
                accessor_shard_helpers.shard_check_error(func_name,
                    tuples, err)
                out.fetches_cnt = 1
                out.fetched_tuples_cnt = #tuples

                -- create iterator
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
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @return index or nil
local function get_primary_index(self, collection_name)
    check(self, 'self', 'table')
    return self.funcs.get_index(self, collection_name, 0)
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
---     return default(self, collection_name, tuple)
---
local function unflatten_tuple(self, collection_name, tuple, opts, default)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    if opts.use_tomap then
        return tuple:tomap({ names_only = true })
    end

    return default(self, collection_name, tuple)
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
    local func_name = 'accessor_shard.insert_tuple'
    check(self, 'self', 'table')

    shard_check_status(func_name)

    local result, err = shard:insert(collection_name, tuple)
    accessor_shard_helpers.shard_check_error(func_name, result, err)

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

--- Update a tuple with an update statements.
---
--- In case when the update should change the storage where the tuple stored
--- we perform insert to the new storage and delete from the old one. The
--- order must be 'first insert, then delete', because insert can report an
--- error in case of unique index constraints violation and we must not
--- perform delete in the case.
---
--- This function emulates (more or less preciselly, see below) behaviour of
--- update as if it would be performed on a local tarantool instance. In case
--- when the tuple resides on the same storage the update operation performs
--- a unique index constraints check within the storage, but not on the overall
--- cluster. In case when the tuple changes its storage the insert operation
--- performs the check within the target storage.
---
--- We can consider this as relaxing of the constraints: the function can
--- silently violate cluster-wide uniqueness constraints or report a
--- violation that was introduced by some previous operation, but cannot
--- report a violation when a local tarantool space would not.
---
--- 'Insert, then delete' approach is applicable and do not lead to a false
--- positive unique index constraint violation when storage nodes are different
--- and do not contain same tuples. We check the first condition in the
--- function and the second is guaranteed by the shard module.
---
--- Note: if one want to use this function as basis for a similar one, but
--- allowing update of a primary key the following details should be noticed. A
--- primary key update that **changes a storage** where the tuple saved can be
--- performed with the 'insert, then delete' approach. An update **within one
--- storage** cannot be performed in the following ways:
---
--- * as update (because tarantool forbids update of a primary key),
--- * 'insert, then delete' way (because insert can report a unique index
---   constraint violation due to values in the old version of the tuple),
--- * 'tuple:update(), then replace' (at least because old tuple resides in the
---   storage and because an other tuple can be silently rewritten).
---
--- To support primary key update for **one storage** case one can use 'delete,
--- then insert' way and perform the rollback action (insert old tuple) in case
--- when insert of the new tuple reports an error. There are other ways, e.g.
--- manual unique constraints check.
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
--- * tuple (cdata/table, optional); a user can provide the original tuple to
---   save one broadcast request performing to determine shard key / needed
---   replica set
---
--- @treturn cdata/table `tuple`
local function update_tuple(self, collection_name, key, statements, opts)
    local func_name = 'accessor_shard.update_tuple'
    check(self, 'self', 'table')

    local opts = opts or {}
    check(opts, 'opts', 'table')
    check(opts.tuple, 'opts.tuple', 'nil', 'cdata', 'table')

    shard_check_status(func_name)

    -- We follow tarantool convention and disallow update of primary key parts.
    local primary_index_info = accessor_shard_index_info.get_index_info(
        collection_name, 0)
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

    local is_shard_key_to_be_updated = false
    local new_shard_key_value
    for _, statement in ipairs(statements) do
        -- statement is {operator, field_no, value}
        local field_no = statement[2]
        if field_no == SHARD_KEY_FIELD_NO then
            is_shard_key_to_be_updated = true
            new_shard_key_value = statement[3]
            break
        end
    end

    local tuple = opts.tuple or get_tuple(self, collection_name, key)

    local is_storage_to_be_changed = false
    if is_shard_key_to_be_updated then
        local old_shard_key_value = tuple[1]
        local old_shard_key_hash = get_shard_key_hash(old_shard_key_value)
        local new_shard_key_hash = get_shard_key_hash(new_shard_key_value)
        is_storage_to_be_changed = old_shard_key_hash ~= new_shard_key_hash
    end

    if is_storage_to_be_changed then
        -- different storages case
        local old_tuple = opts.tuple or get_tuple(self, collection_name, key)
        local new_tuple = old_tuple:update(statements)
        self.funcs.insert_tuple(self, collection_name, new_tuple)
        self.funcs.delete_tuple(self, collection_name, key, {tuple = old_tuple})
        return new_tuple
    else
        -- one storage case
        local nodes = shard.shard(tuple[SHARD_KEY_FIELD_NO])
        local tuple = space_operation(collection_name, nodes, 'update', key,
            statements)
        assert(tuple ~= nil,
            ('cannot find a tuple in collection "%s" by the primary key %s ' ..
            'to update'):format(collection_name, json.encode(key)))
        return tuple
    end
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
--- * tuple (cdata/table, optional); the same as in @{update_tuple}
---
--- @treturn cdata tuple
local function delete_tuple(self, collection_name, key, opts)
    local func_name = 'accessor_shard.delete_tuple'
    check(self, 'self', 'table')

    local opts = opts or {}
    check(opts, 'opts', 'table')
    check(opts.tuple, 'opts.tuple', 'nil', 'cdata', 'table')

    shard_check_status(func_name)

    local tuple = opts.tuple or get_tuple(self, collection_name, key)
    local nodes = shard.shard(tuple[SHARD_KEY_FIELD_NO])
    local tuple = space_operation(collection_name, nodes, 'delete', key)
    assert(tuple ~= nil,
        ('cannot find a tuple in collection "%s" by the primary key %s ' ..
        'to delete'):format(collection_name, json.encode(key)))
    return tuple
end

--- Fetch data to the cache.
---
--- @tparam table self accessor_general instance
---
--- @tparam table batches see @{accessor_shard_cache.cache_fetch}
---
--- @treturn table see @{accessor_shard_cache.cache_fetch}
local function cache_fetch(self, batches)
    return self.data_cache:fetch(batches)
end

-- Unused for now.
-- --- Delete fetched data by fetch_id.
-- ---
-- --- @tparam table self accessor_general instance
-- ---
-- --- @tparam number fetch_id identifier of the fetched data
-- ---
-- --- @return nothing
-- local function cache_delete(self, fetch_id)
--     self.data_cache:delete(fetch_id)
-- end

--- Delete all fetched data.
---
--- @tparam table self accessor_general instance
---
--- @return nothing
local function cache_truncate(self)
    self.data_cache:truncate()
end

--- Lookup for data in the cache.
---
--- @tparam table self accessor_general instance
---
--- @tparam string collection_name
---
--- @tparam string index_name
---
--- @param key
---
--- @tparam table iterator_opts e.g. {} or {iterator = 'GT'}
---
--- @return luafun iterator (one value) to fetched data or nil
local function cache_lookup(self, collection_name, index_name,
        key, iterator_opts)
    return self.data_cache:lookup(collection_name, index_name, key,
        iterator_opts)
end

--- Create a new shard data accessor instance.
function accessor_shard.new(opts, funcs)
    local funcs = funcs or {}
    check(funcs, 'funcs', 'table')
    -- assert(shard ~= nil,
    --     'tarantool/shard module is needed to working with accessor_shard')

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
        cache_fetch = funcs.cache_fetch or cache_fetch,
        -- cache_delete = funcs.cache_delete or cache_delete,
        cache_truncate = funcs.cache_truncate or cache_truncate,
        cache_lookup = funcs.cache_lookup or cache_lookup,
    }

    local opts = table.copy(opts)
    opts.name = 'shard'
    opts.data_cache = accessor_shard_cache.new()
    return accessor_general.new(opts, res_funcs)
end

return accessor_shard

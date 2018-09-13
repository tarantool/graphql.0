--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as vshard accessor and provides the
--- `accessor_vshard.new` function to create a new vshard data accessor
--- instance.

local json = require('json')
local utils = require('graphql.utils')
local clock = require('clock')
local vshard = utils.optional_require('vshard')
local accessor_general = require('graphql.accessor_general')

local check = utils.check

local accessor_vshard = {}

local LIMIT = 100000 -- XXX: we need to raise an error when a limit reached

--- Wraps a simple router:call to handle errors.
local function space_call(self, bucket_id, mode, args, opts)
    local ret, err = self.router:call(bucket_id, mode, 'space_call',
        args, opts)
    if ret == nil and err ~= nil then
        assert(err.message, 'vshard error should have a message')
        error(err.message)
    end
    return ret
end

--- Get numbers of index parts in a tuple.
--- This function assumes that
--- @treturn table {<number, part_1> [<number, part_N>]}
local function get_index_parts(self, xspace, xindex, retry)
    retry = retry == nil and true or retry
    -- All replicas should have the same schema.
    local _, replicaset = next(self.router.replicasets)
    assert(replicaset, 'Vshard should have at least one replicaset')
    local _, replica = next(replicaset.replicas)
    assert(replicaset, 'Vshard replicaset should have at least one replica')
    local conn = replica.conn
    local space = conn.space[xspace]
    local index
    if space == nil and retry then
        goto lretry
    end
    assert(space, ('space %s do not exist on Vshard storage'):format(space))
    index = space.index[xindex]
    if index == nil and retry then
        goto lretry
    end
    if index then
        local parts = {}
        -- TODO: support json path indexes.
        for _, part in ipairs(index.parts) do
            -- TODO: respect collation
            table.insert(parts, part.fieldno)
        end
        return parts
    end
    error('Index not found on storage')
::lretry::
    -- Handle schema update.
    conn:ping()
    return get_index_parts(self, space, index, false)
end

-- Generate comparator function which compares tuples in the same way the
-- index does.
local function get_comparator(self, collection_name, index)
    -- Comparator cache is preallocated for all spaces.
    local space_cache = self.comparator_cache[collection_name]
    assert(space_cache)
    local comparator = space_cache[index]
    if comparator then
        return comparator
    end
    local parts = get_index_parts(self, collection_name, index)
    -- TODO: implement avro-like generator for fast
    -- comparators.
    comparator = function(a, b)
        for _, i in ipairs(parts) do
            if a[i] ~= b[i] then
                if a[i] == nil then
                    return true
                end
                if b[i] == nil then
                    return false
                end
                return a[i] < b[i]
            end
        end
        return false
    end
    space_cache[index] = comparator
    return comparator
end

-- Get a bucket_id for an object based on:
-- 1. bucket_id field
-- 2. parent object
-- 3. object itself/filter
local function get_bucket_id(self, collection_name, op_opts)
    local parent = op_opts.parent
    local from = op_opts.from
    -- Attempt to get bucket_id from query parameters.
    local obj_attributes = op_opts.raw_filter -- For query.
        or op_opts.object -- For mutations.
    local bucket_id = obj_attributes[self.vshard[collection_name].bucket_id_field]
    if bucket_id then
        return bucket_id
    end

    -- Attempt to get calculated bucket_id from parent.
    if parent and from.collection_name then
        local vshard_parent = self.vshard[from.collection_name]
        assert(vshard_parent)
        if vshard_parent.bucket_local_connections[from.connection_name] then
            bucket_id = parent[vshard_parent.bucket_id_field]
            assert(bucket_id)
            return bucket_id
        end
    end

    -- Attempt to get bucket_id from filter.
    -- TODO: do not delete any parameters from a filter object
    -- (by now it is necessary to maintain raw_filter)
    local vshard_cfg = self.vshard[collection_name]
    assert(vshard_cfg)
    if not vshard_cfg.key_fields then
        return nil
    end
    local args = {}
    for _, k in ipairs(vshard_cfg.key_fields) do
        local field
        field = obj_attributes[k]
        if not field then
            return nil
        end
        table.insert(args, field)
    end
    bucket_id = vshard_cfg.get_bucket_id(unpack(args))
    return bucket_id
end

local function get_timeout(deadline_clock)
    local timeout = deadline_clock - clock.monotonic64()
    -- Convert from ns (cdata).
    timeout = tonumber(timeout) / 10 ^ 9
    return timeout
end


--- Make a call to a single replicaset (in case bucket_id in found) or
--- to the whole cluster.
--- @tparam table self accessor instance
--- @tparam string collection_name
--- @tparam string index
--- @tparam table key
--- @tparam table opts
--- @tparam table select_opts
--- @treturn table Array of selected tuples
local function space_call_scan(self, collection_name, index, key, opts,
        select_opts)
    local bucket_id = get_bucket_id(self, collection_name, select_opts)
    if bucket_id then
        local timeout = get_timeout(select_opts.deadline_clock)
        local ret = space_call(self, bucket_id, 'read',
            {collection_name, index, 'select', key, opts}, {timeout = timeout})
        return ret
    else
        local comparator = get_comparator(self, collection_name, 0)
        local replicasets = self.router:routeall()
        local res = {}
        for _, rs in pairs(replicasets) do
            local timeout = get_timeout(select_opts.deadline_clock)
            local ret, err = rs:callrw('space_call',
                {collection_name, index, 'select', key, opts},
                {timeout = timeout})
            if ret == nil and err ~= nil then error(err) end
            table.insert(res, ret)
        end
        -- TODO: use heap for the sort.
        res = utils.merge_arrays(res)
        table.sort(res, comparator)
        assert(opts.limit, "limit is " .. tostring(opts.limit))
        res = utils.subarray(res, 1, opts.limit)
        return res
    end
end

--- See detailed documentation here @{accessor_shard.is_collection_exists}
local function is_collection_exists(self, collection_name)
    return true
end

--- See detailed documentation here @{accessor_shard.is_index_exists}
local function is_index_exists()
    return true
end

local function get_index(self, collection_name, index_name)
    check(self, 'self', 'table')
    if not is_index_exists(collection_name, index_name) then
        return nil
    end

    local index = setmetatable({}, {
        __index = {
            pairs = function(xself, value, opts, out, select_opts)
                local opts = opts or {}
                opts.limit = opts.limit or LIMIT
                local tuples = space_call_scan(self, collection_name,
                    index_name, value, opts, select_opts)
                out.fetches_cnt = 1
                out.fetched_tuples_cnt = #tuples
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

local function get_primary_index(self, collection_name)
    check(self, 'self', 'table')
    return self.funcs.get_index(self, collection_name, 0)
end

--- See detailed documentation here @{accessor_shard.unflatten_tuple}
local function unflatten_tuple(self, collection_name, tuple, opts, default)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    if opts.use_tomap then
        return tuple:tomap({ names_only = true })
    end

    return default(self, collection_name, tuple)
end

--- See detailed documentation here @{accessor_shard.flatten_object}
local function flatten_object(self, collection_name, obj, opts, default,
        op_opts)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local bucket_id_field = self.vshard[collection_name].bucket_id_field
    if not obj[bucket_id_field] then
        local bucket_id = get_bucket_id(self, collection_name, op_opts)
        if not bucket_id then
            error('Cannot find or infer bucket id for object ' ..
                require('json').encode(op_opts.object))
        end
        obj[bucket_id_field] = bucket_id
    end
    local service_fields_defaults = opts.service_fields_defaults or {}
    check(service_fields_defaults, 'service_fields_defaults', 'table')
    return default(self, collection_name, obj, opts)
end

--- See detailed documentation here @{accessor_shard.xflatten}
local function xflatten(self, collection_name, xobject, opts, default)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local service_fields_defaults = opts.service_fields_defaults or {}
    check(service_fields_defaults, 'service_fields_defaults', 'table')
    return default(self, collection_name, xobject, opts)
end

--- See detailed documentation here @{accessor_shard.insert_tuple}
local function insert_tuple(self, collection_name, tuple, insert_opts)
    check(self, 'self', 'table')

    -- Bucket id should be already prepared by flatten.
    local bucket_id =
        insert_opts.object[self.vshard[collection_name].bucket_id_field]
    assert(bucket_id, 'Bucket id should have been prepared by flatten func')

    local ret = space_call(self, bucket_id, 'write',
        {collection_name, nil, 'insert', tuple})
    return ret
end

--- See detailed documentation here @{accessor_shard.update_tuple}
local function update_tuple(self, collection_name, key, object, statements,
        opts)
    check(self, 'self', 'table')
    local opts = opts or {}
    check(opts, 'opts', 'table')
    check(opts.tuple, 'opts.tuple', 'nil', 'cdata', 'table')
    local bucket_id_fname = self.vshard[collection_name].bucket_id_field
    local bucket_related_fields = {
        [bucket_id_fname] = true
    }
    local key_fields = self.vshard[collection_name].key_fields
    if key_fields then
        for _, fname in ipairs(key_fields) do
            bucket_related_fields[fname] = true
        end
    end

    assert(opts.xobject)
    for fname, _ in pairs(opts.xobject) do
        -- statement is {operator, field_no, value}
        if bucket_related_fields[fname] then
            error(("Attempt to modify a tuple field " ..
                "'%s' which is related to bucket_id in space '%s'"):format(
                    fname, collection_name))
        end
    end

    local bucket_id = object[self.vshard[collection_name].bucket_id_field]
    assert(bucket_id, 'Bucket id should be in the object to update')
    local ret = space_call(self, bucket_id, 'write',
        {collection_name, nil, 'update', key, statements})
    return ret
end

--- See detailed documentation here @{accessor_shard.delete_tuple}
local function delete_tuple(self, collection_name, key, object)
    local bucket_id = object[self.vshard[collection_name].bucket_id_field]
    assert(bucket_id, 'Bucket id should be in the object to delete')
    local tuple = space_call(self, bucket_id, 'write',
        {collection_name, nil, 'delete', key})
    assert(tuple ~= nil,
        ('cannot find a tuple in collection "%s" by the primary key %s ' ..
        'to delete'):format(collection_name, json.encode(key)))
    return tuple
end

--- See detailed documentation here @{accessor_shard.new}
function accessor_vshard.new(uopts, funcs)
    local router = uopts.router
    uopts.router = nil
    local opts = table.deepcopy(uopts)
    uopts.router = router
    opts.router = router
    opts.name = 'vshard'
    local funcs = funcs or {}
    check(funcs, 'funcs', 'table')
    check(opts.vshard, 'cfg.vshard', 'table')
    assert(vshard ~= nil,
        'tarantool/vshard module is needed to working with accessor_vshard')

    for k, v in pairs(funcs) do
        check(k, 'funcs key', 'string')
        check(v, 'funcs value', 'function')
    end

    -- Convert vshard cfg to indexable form.
    -- TODO: preserve user input untouched.
    for space_name, space in pairs(opts.vshard) do
        check(space.key_fields, 'key_fields', 'table', 'nil')
        check(space.get_bucket_id, 'get_bucket_id', 'function', 'nil')
        check(space.bucket_id_field, 'bucket_id_field', 'string')
        space.bucket_id_field = space.bucket_id_field or 'bucket_id'
        local bucket_local_connections = {}
        space.bucket_local_connections = space.bucket_local_connections or {}
        for _, connection in ipairs(space.bucket_local_connections) do
            bucket_local_connections[connection] = true
        end
        space.bucket_local_connections = bucket_local_connections
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

    local accessor = accessor_general.new(opts, res_funcs)
    accessor.router = opts.router
    accessor.comparator_cache = {}
    for space_name, _ in pairs(accessor.collections) do
        accessor.comparator_cache[space_name] = {}
    end
    return accessor
end

return accessor_vshard

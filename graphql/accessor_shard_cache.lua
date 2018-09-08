local json = require('json')
local utils = require('graphql.utils')
local shard = utils.optional_require('shard')
local request_batch = require('graphql.request_batch')
local accessor_shard_index_info = require('graphql.accessor_shard_index_info')

local check = utils.check

local accessor_shard_cache = {}

local function get_select_ids(self, batch, opts)
    local opts = opts or {}
    local skip_cached = opts.skip_cached or false

    local skip_function
    if skip_cached then
        skip_function = function(id)
            return self.cache[id] ~= nil
        end
    end

    return batch:select_ids(skip_function)
end

local function net_box_call_wrapper(conn, func_name, call_args)
    local ok, result = pcall(conn.call, conn, func_name, call_args,
        {is_async = true})

    if not ok then
        return nil, {
            message = ('%s: %s'):format(func_name, json.encode(result)),
            extensions = {
                net_box_error = result,
            }
        }
    end

    return result
end

local function is_future(result)
    return type(result) == 'table' and type(result.wait_result) == 'function'
end

local function cache_fetch_batch(self, batch, fetch_id, stat)
    local ids = get_select_ids(self, batch, {skip_cached = true})
    if next(ids) == nil then return end

    -- perform requests
    local results_per_replica_set = {}
    for i, replica_set in ipairs(shard.shards) do
        local first_err

        -- perform the request on each node in a replica set starting from
        -- a master node until success or end of the nodes
        for n = #replica_set, 1, -1 do
            local node = replica_set[n]
            local conn = node.conn
            local call_args = {
                batch.collection_name,
                batch.index_name,
                batch.keys,
                batch.iterator_opts
            }
            local node_results, node_err = net_box_call_wrapper(conn,
                'batch_select', call_args)

            if node_results == nil then
                if first_err == nil then
                    first_err = node_err
                end
            else
                results_per_replica_set[i] = node_results
                break -- go to the next replica_set
            end
        end

        -- no successful requests, return the error from the master node
        if first_err ~= nil then
            error(first_err)
        end
    end

    -- merge results without sorting: transform
    -- results_per_replica_set[replica_set_num][request_num] 2d-array into
    -- results[request_num] 1d-array
    local results = {}
    for _, node_results in ipairs(results_per_replica_set) do
        if is_future(node_results) then
            node_results = node_results:wait_result()[1]
        end
        -- we cannot use C merger for now, because buffers will contain
        -- list of list of tuples instead of list of tuples
        for j, node_result in ipairs(node_results) do
            results[j] = results[j] or {}
            for _, tuple in ipairs(node_result) do
                table.insert(results[j], tuple)
            end
        end
    end

    -- count fetches: assume a fetch as one request across cluster (don't
    -- count individulal requests to storages)
    stat.fetches_cnt = stat.fetches_cnt + 1
    -- count full scan and index lookup counts
    for i, key in ipairs(batch.keys) do
        if key == nil or (type(key) == 'table' and
                next(key) == nil) then
            stat.full_scan_cnt = stat.full_scan_cnt + 1
        else
            stat.index_lookup_cnt = stat.index_lookup_cnt + 1
        end
    end

    -- sort by a primary key
    local primary_index_info = accessor_shard_index_info.get_index_info(
        batch.collection_name, 0)
    for _, result in ipairs(results) do
        -- count fetched tuples
        stat.fetched_tuples_cnt = stat.fetched_tuples_cnt + #result
        table.sort(result, function(a, b)
            for i, part in pairs(primary_index_info.parts) do
                if a[part.fieldno] ~= b[part.fieldno] then
                    return a[part.fieldno] < b[part.fieldno]
                end
            end
            return false
        end)
    end

    -- write to cache
    assert(#results == #batch.keys,
        ('results count %d is not the same as requests count %d'):format(
        #results, #batch.keys))
    for i = 1, #ids do
        if self.cache[ids[i]] == nil then
            self.cache[ids[i]] = {
                result = results[i],
                fetch_ids = {fetch_id}
            }
        else
            -- XXX: we can skip this key from a request
            for j, tuple in ipairs(self.cache[ids[i]].result) do
                if not utils.are_tables_same(results[i][j], tuple) then
                    error(('fetched tuple %s is not the same as one in ' ..
                        'the cache: %s'):format(json.encode(tuple),
                        json.encode(results[i][j])))
                end
            end
            table.insert(self.cache[ids[i]].fetch_ids, fetch_id)
        end
    end
end

--- Fetch data to the cache.
---
--- @tparam table self accessor_shard_cache instance
---
--- @tparam table batches requests batches in the following format:
---
---     batches = {
---         [field_name] = {
---             collection_name = <string>,
---             index_name = <string or nil>,
---             keys = <...>,
---             iterator_opts = <...>,
---         },
---         ...
---     }
---
--- @treturn table the following structure or nil:
---
---     {
---         fetch_id = <number>, -- identifier of the fetched data
---         stat = {             -- data to update statistics
---             fetches_cnt = <number>,
---             fetched_tuples_cnt = <number>,
---             full_scan_cnt = <number>,
---             index_lookup_cnt = <number>,
---         }
---     }
local function cache_fetch(self, batches)
    local fetch_id = self.fetch_id_next
    self.fetch_id_next = self.fetch_id_next + 1

    local stat = {
        fetches_cnt = 0,
        fetched_tuples_cnt = 0,
        full_scan_cnt = 0,
        index_lookup_cnt = 0,
    }

    for _, batch in pairs(batches) do
        cache_fetch_batch(self, batch, fetch_id, stat)
    end

    return {
        fetch_id = fetch_id,
        stat = stat,
    }
end

-- Unused for now.
-- --- Delete fetched data by fetch_id.
-- ---
-- --- @tparam table self accessor_shard_cache instance
-- ---
-- --- @tparam number fetch_id identifier of the fetched data
-- ---
-- --- @return nothing
-- local function cache_delete(self, fetch_id)
--     local ids_to_remove = {}
--
--     for id, item in pairs(self.cache) do
--         if #item.fetch_ids == 1 and item.fetch_ids[1] == fetch_id then
--             table.insert(ids_to_remove, id)
--         elseif #item.fetch_ids > 1 then
--             local fetch_ids_to_remove = {}
--             for i, fid in ipairs(item.fetch_ids) do
--                 if fid == fetch_id then
--                     table.insert(fetch_ids_to_remove, i)
--                 end
--             end
--             table.sort(fetch_ids_to_remove, function(a, b) return a > b end)
--             for _, i in ipairs(fetch_ids_to_remove) do
--                 table.remove(item.fetch_ids, i)
--             end
--         end
--     end
--
--     for _, id in ipairs(ids_to_remove) do
--         self.cache[id] = nil
--     end
-- end

--- Delete all fetched data.
---
--- @tparam table self accessor_shard_cache instance
---
--- @return nothing
local function cache_truncate(self)
    self.cache = {}
end

--- Lookup for data in the cache.
---
--- @tparam table self accessor_shard_cache instance
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
local function cache_lookup(self, collection_name, index_name, key,
        iterator_opts)
    local batch = request_batch.new(collection_name, index_name,
        {key or box.NULL}, iterator_opts)
    local id = get_select_ids(self, batch)[1]
    if self.cache[id] == nil then
        return nil
    end

    local tuples = self.cache[id].result
    check(tuples, 'tuples', 'table')
    -- XXX: wrap all data into the table, don't create the capture
    return setmetatable({tuples = tuples}, {
        __index = {
            pairs = function(self, value, opts, out)
                assert(value == key, 'expected the same key in ' ..
                    'cache_lookup call and :pairs() on returned iterable')
                assert(opts == iterator_opts, 'expected the same ' ..
                    'iterator_opts in cache_lookup call and :pairs() on ' ..
                    'returned iterable')

                out.cache_hits_cnt = 1
                out.cache_hit_tuples_cnt = #self.tuples

                -- create iterator
                local cur = 1
                local function gen()
                    if cur > #self.tuples then return nil end
                    local res = tuples[cur]
                    cur = cur + 1
                    return cur, res
                end

                return gen, nil, nil
            end
        }
    })
end

--- Create new accessor_shard_cache instance.
---
--- @treturn table accessor_shard_cache instance
function accessor_shard_cache.new()
    return setmetatable({
        cache = {},
        fetch_id_next = 1,
    }, {
        __index = {
            fetch = cache_fetch,
            -- Unused for now.
            -- delete = cache_delete,
            truncate = cache_truncate,
            lookup = cache_lookup,
        },
    })
end

return accessor_shard_cache

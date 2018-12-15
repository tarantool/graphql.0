--- Implements cache of index information (parts and its properties).

local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')
local shard = utils.optional_require('shard')
local accessor_shard_helpers = require('graphql.accessor_shard_helpers')
local key_def_lib = utils.optional_require('key_def')

local accessor_shard_index_info = {}

-- XXX: accessor_shard_index_info.new()

local index_info_cache = {}
local key_def_cache = {}

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

--- Get an index object from net_box under the shard module.
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
function accessor_shard_index_info.get_index_info(collection_name, index_name)
    local func_name = 'accessor_shard_index_info.get_index_info'
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
            accessor_shard_helpers.shard_check_error(func_name, result, err)
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

--- Create or get cached key_def for given collection and index.
---
--- XXX: Implement some cache clean up strategy and a way to manual cache
--- purge.
---
--- @tparam string collection_name
---
--- @tparam string index_name
---
--- @treturn cdata `key_def`
function accessor_shard_index_info.get_key_def(collection_name,
        index_name)
    local func_name = 'accessor_shard_index_info.get_key_def'

    if key_def_lib == nil then
        error(('internal error: %s: key_def is requested, but is not ' ..
            'supported by the current tarantool version'):format(func_name))
    end

    -- get from the cache if exists
    if key_def_cache[collection_name] ~= nil then
        local key_def = key_def_cache[collection_name][index_name]
        if key_def ~= nil then
            return key_def
        end
    end

    local index_info = accessor_shard_index_info.get_index_info(collection_name,
        index_name)
    local key_def = key_def_lib.new(index_info.parts)
    if not index_info.unique then
        local primary_index_info = accessor_shard_index_info.get_index_info(
            collection_name, 0)
        key_def = key_def:merge(key_def_lib.new(primary_index_info.parts))
    end

    -- write to the cache
    if key_def_cache[collection_name] == nil then
        key_def_cache[collection_name] = {}
    end
    key_def_cache[collection_name][index_name] = key_def

    return key_def
end

return accessor_shard_index_info

--- The module implements functions needed to make general accessor
--- (@{accessor_general}) behaves as shard accessor and provides the
--- `accessor_shard.new` function to create a new shard data accessor instance.

local shard = require('shard')
local accessor_general = require('graphql.accessor_general')

local accessor_shard = {}

local LIMIT = 100000 -- XXX: we need to raise an error when a limit reached

-- XXX: sorting list results (by primary key?)

-- Check whether a collection (it is sharded space for that accessor) exists.
local function is_collection_exists(collection_name)
    local exists
    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            local cur = shard:space_call(collection_name, node,
                function(space_obj)
                    return space_obj ~= nil
                end)
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
    local exists
    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            local cur = shard:space_call(collection_name, node,
                function(space_obj)
                    return space_obj.index[index_name] ~= nil
                end)
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
            pairs = function(self, value)
                local tuples = shard:secondary_select(collection_name,
                    index_name, value, {limit = LIMIT})
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

--- Create a new shard data accessor instance.
function accessor_shard.new(opts)
    local funcs = {
        is_collection_exists = is_collection_exists,
        get_index = get_index,
        get_primary_index = get_primary_index,
    }
    return accessor_general.new(opts, funcs)
end

return accessor_shard

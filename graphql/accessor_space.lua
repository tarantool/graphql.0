local accessor_general = require('graphql.accessor_general')

local accessor_space = {}

-- Check whether a collection (it is space for that accessor) exists.
local function is_collection_exists(collection_name)
    return box.space[collection_name] ~= nil
end

--- Get index to perform :pairs({v1, ...})
--- @return index or nil
local function get_index(collection_name, index_name)
    return box.space[collection_name].index[index_name]
end

--- Get primary index to perform :pairs() (fullscan)
local function get_primary_index(collection_name)
    return box.space[collection_name].index[0]
end

function accessor_space.new(opts)
    local funcs = {
        is_collection_exists = is_collection_exists,
        get_index = get_index,
        get_primary_index = get_primary_index,
    }
    return accessor_general.new(opts, funcs)
end

return accessor_space

local utils = require('graphql.utils')
local check = utils.check

local db_schema_helpers = {}

--- Get user-provided meta-information about the primary index of given
--- collection.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @tparam string collection_name the name of collection to find the primary
--- index
---
--- @treturn string `index_name`
---
--- @treturn table `index` (meta-information, not the index itself)
function db_schema_helpers.get_primary_index_meta(db_schema, collection_name)
    check(db_schema, 'db_schema', 'table')
    check(collection_name, 'collection_name', 'string')

    local indexes = db_schema.indexes[collection_name]

    local res_index_name

    for index_name, index in pairs(indexes) do
        if res_index_name == nil and index.primary then
            res_index_name = index_name
        elseif res_index_name ~= nil and index.primary then
            error(('several indexes were marked as primary in ' ..
                'the "%s" collection, at least "%s" and "%s"'):format(
                collection_name, res_index_name, index_name))
        end
    end

    if res_index_name == nil then
        error(('cannot find primary index for collection "%s"'):format(
            collection_name))
    end

    local res_index = indexes[res_index_name]
    return res_index_name, res_index
end

--- Get schema name by a collection name.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @tparam string collection_name
---
--- @treturn string `schema_name`
function db_schema_helpers.get_schema_name(db_schema, collection_name)
    local collection = db_schema.collections[collection_name]
    assert(collection ~= nil,
        ('cannot find the collection "%s"'):format(collection_name))
    local schema_name = collection.schema_name
    check(schema_name, 'schema_name', 'string')
    return schema_name
end

return db_schema_helpers

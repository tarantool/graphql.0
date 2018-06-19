--- Config complement module provides an ability to complement user-defined config
--- (in a simplified format) to a fully specified format.
---
--- Notes:
---
--- * Currently the module complements only connections (cfg.connections),
--- see @{complement_connections}.

local json = require('json')
local yaml = require('yaml')
local log = require('log')

local utils = require('graphql.utils')
local check = utils.check

local config_complement = {}

--- Determine connection type by connection.parts and index uniqueness.
local function determine_connection_type(connection_parts, index)
    if #connection_parts < #index.fields then
        return '1:N'
    elseif #connection_parts == #index.fields then
        return index.unique and '1:1' or '1:N'
    end

    error(('Connection parts count is more then index parts count: %d > %d')
        :format(#connection_parts, #index.fields))
end

-- Return connection_parts sorted by destination_fields as index_fields prefix.
local function sort_parts(connection_parts, index_fields)
    local sorted_parts = {}

    -- check if fields in connection_parts exist in index_fields
    for _, part in ipairs(connection_parts) do
        local is_found = false
        for i, index_field in ipairs(index_fields) do
            if part.destination_field == index_field then
                is_found = true
            end
        end
        assert(is_found, ('part.destination_field %s was not found in ' ..
            'connection index %s'):format(part.destination_field,
                json.encode(index_fields)))
    end

    -- sort parts and check that sorted_parts form index prefix
    -- (including index itself)
    for i = 1, utils.table_size(connection_parts) do
        local index_field = index_fields[i]
        for _, part in ipairs(connection_parts) do
            if part.destination_field == index_field then
                sorted_parts[i] = {destination_field = part.destination_field,
                                   source_field = part.source_field}
                break
            end
            -- no match found
            error(('given parts %s does not form an index or an index ' ..
                'prefix %s'):format(json.encode(connection_parts),
                    json.encode(index_fields)))
        end
    end
    return sorted_parts
end

local function is_order_different(connection_parts, index_fields)
    for i, _ in ipairs(connection_parts) do
        if connection_parts[i].destination_field ~= index_fields[i] then
            return true
        end
    end
    return false
end

--- The function complements partially defined (nil/number) connection parts
--- or check and sort fully defined (table) connection parts.
--- @tparam table parts partially defined connection's part given by user
--- @tparam table index connection index (cfg.indexes[collection][index_name])
--- index.fields will be used as the source of information about the index parts order.
--- An error will be raised in cases when parts is a table and cannot form a
--- prefix of index.fields. When parts can be resorted to fit right order, they
--- will be resorted.
local function determine_connection_parts(parts, index)
    check(parts, 'parts', 'nil', 'number', 'table')
    local result_parts = {}

    -- User defined no parts of the connection. All connection's index fields
    -- are taken as 'parts'
    if type(parts) == 'nil' then
        for i, v in ipairs(index.fields) do
            result_parts[i] = {source_field = v, destination_field = v}
        end
    end

    -- User defined a number of fields of index which must form index prefix.
    -- First 'number' index fields are taken as 'parts'
    if type(parts) == 'number' then
        for i = 1, parts do
            local v = index.fields[i]
            result_parts[i] = {source_field = v, destination_field = v}
        end
    end

    -- User defined parts as pairs of {source_field: foo_field,
    -- destination_field: boo_field}. These 'parts' may correspond either to full
    -- index or index prefix
    if type(parts) == 'table' then
        -- sorting parts is necessary to check if user defined part form an
        -- index or an index prefix
        if is_order_different(parts, index) then
            log.warn(('Parts \n %s \n were given in the wrong order and ' ..
                'sorted to match the right order of destination collection ' ..
                'index fields \n %s \n'):format(yaml.encode(parts),
                    yaml.encode(index.fields)))
            result_parts = sort_parts(parts, index.fields)
        else
            result_parts = parts
        end
    end

    return result_parts
end

--- The function complements collections' connections, described in simplified
--- format, to connections in a fully specified format. Type determined on index type.
--- Each connection will be added to a `source_collection' collection,
--- because the format of a collection assumes inclusion of all outcoming connections.
--- Notice an example:
---
---     "connections" : [
---         {
---             "name": "order_connection",
---             "source_collection": "user_collection",
---             "destination_collection": "order_collection"
---             "index_name": "user_id_index",
---             "parts" : nil | number | table (destination fields can be omitted)
---                 in case of 'table' expected format is:
---                 "parts": [
---                     {"source_field": "user_id", "destination_field": "user_id"},
---                     ...
---                 ]
---         },
---         ...
---     ]
---
--- will produce following complement in 'user_collection' :
---
---     "user_collection": {
---         "schema_name": "user",
---         "connections": [
---             {
---                 "type": "1:N",
---                 "name": "order_connection",
---                 "destination_collection":  "order_collection",
---                 "parts": [
---                     { "source_field": "user_id", "destination_field": "user_id" }
---                 ],
---                 "index_name": "user_id_index"
---             },
---         ]
---     }
---
--- @tparam table collections cfg.collections (will be changed in place)
--- @tparam table connections cfg.connections - user-defined collections
--- @tparam table indexes cfg.indexes - {[collection_name] = collection_indexes, ...}
--- @treturn table `collections` is complemented collections
local function complement_connections(collections, connections, indexes)
    if connections == nil then
        return collections
    end

    check(collections, 'collections', 'table')
    check(connections, 'connections', 'table')

    for _, c in pairs(connections) do
        check(c.name, 'connection.name', 'string')
        check(c.source_collection, 'connection.source_collection', 'string')
        check(c.destination_collection, 'connection.destination_collection',
            'string')
        check(c.index_name, 'connection.index_name', 'string')
        check(c.parts, 'connection.parts', 'string', 'table', 'nil')

        local index = indexes[c.source_collection][c.index_name]
        assert(index.unique ~= nil, 'index.unique must not be nil ' ..
            'during connections complementing')

        local result_c = {}
        result_c.source_collection = c.source_collection
        result_c.destination_collection = c.destination_collection
        result_c.parts = determine_connection_parts(c.parts, index)

        result_c.type = determine_connection_type(result_c.parts, index)
        result_c.index_name = c.index_name
        result_c.name = c.name

        local collection_connections = collections[c.source_collection].
            connections or {}
        collection_connections[#collection_connections + 1] = result_c
    end
    return collections
end

--- The function complements cfg.collection.connections using given
--- cfg.connections. See @{complement_connections} for details.
function config_complement.complement_cfg(cfg)
    cfg.collections = complement_connections(cfg.collections, cfg.connections,
        cfg.indexes)
    return cfg
end

return config_complement

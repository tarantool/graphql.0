--- Data accessor module that is base for `accessor_space` and
--- `accessor_shard` ones.
---
--- It provides basic logic for such space-like data storages and abstracted
--- away from details from where tuples are arrived into the application.

local avro_schema = require('avro_schema')
local utils = require('graphql.utils')

local accessor_general = {}

--- Validate and compile set of avro schemas (with respect to service fields).
--- @tparam table schemas map where keys are string names and values are
--- avro schemas; consider an example in @{tarantool_graphql.new} function
--- description.
--- @tparam table service_fields map where keys are string names of avro
--- schemas (from `schemas` argument) and values are service fields descriptions;
--- consider the example in the @{new} function description.
local function compile_schemas(schemas, service_fields)
    local service_fields_types = {}
    for name, service_fields_list in pairs(service_fields) do
        assert(type(name) == 'string',
            'service_fields key must be a string, got ' .. type(name))
        assert(type(service_fields_list) == 'table',
            'service_fields_list must be a table, got ' ..
            type(service_fields_list))
        local sf_types = {}
        for _, v in ipairs(service_fields_list) do
            assert(type(v) == 'table',
                'service_fields_list item must be a table, got ' .. type(v))
            assert(type(v.name) == 'string',
                'service_field name must be a string, got ' .. type(v.name))
            assert(type(v.type) == 'string',
                'service_field type must be a string, got ' .. type(v.type))
            assert(v.default ~= nil, 'service_field default must not be a nil')
            sf_types[#sf_types + 1] = v.type
        end
        service_fields_types[name] = sf_types
    end

    local models = {}
    for name, schema in pairs(schemas) do
        assert(type(schema) == 'table',
            'avro_schema must be a table, got ' .. type(schema))
        assert(type(name) == 'string',
            'schema key must be a string, got ' .. type(name))
        assert(type(schema.name) == 'string',
            'schema name must be a string, got ' .. type(schema.name))
        assert(name == schema.name,
            ('schema key and name do not match: %s vs %s'):format(
            name, schema.name))
        assert(schema.type == 'record', 'schema.type must be a record')

        local ok, handle = avro_schema.create(schema)
        assert(ok, ('cannot create avro_schema for %s: %s'):format(
            name, tostring(handle)))
        local sf_types = service_fields_types[name]
        assert(sf_types ~= nil,
            ('cannot find service_fields for schema "%s"'):format(name))
        local ok, model = avro_schema.compile(
            {handle, service_fields = sf_types})
        assert(ok, ('cannot compile avro_schema for %s: %s'):format(
            name, tostring(model)))

        models[name] = model
    end
    return models
end

-- XXX: support partial match for primary/secondary indexes and support to skip
-- fields to get an index (full_match must be false in the case because
-- returned items will be additionally filtered after unflatten).

--- Choose an index for lookup tuple(s) by a 'filter'. The filter holds fields
--- values of object(s) we want to find. It uses prebuilt `lookup_index_name`
--- table representing available indexes, which created by the
--- `build_lookup_index_name` function.
---
--- @tparam table self the data accessor created by the `new` function
--- (directly or indirectly using the `accessor_space.new` or the
--- `accessor_shard.new` function); this function uses the
--- `self.lookup_index_name` prebuild table representing available indexes

--- @tparam string collection_name name of a collection of whose indexes the
--- function will search through
---
--- @tparam table filter map from fields names to values; names are used for
--- lookup needed index, values forms the `value_list` return value
---
--- @treturn boolean `full_match` is whether passing `value_list` to the index
--- with name `index_name` will give tuple(s) proven to match the filter or
--- just some subset of all tuples in the collection which need to be filtered
--- further
---
--- @treturn string `index_name` is name of the found index or nil
---
--- @treturn table `value_list` is values list from the `filter` argument
--- ordered in the such way that can be passed to the found index (has some
--- meaning only when `index_name ~= nil`)
local get_index_name = function(self, collection_name, filter)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' .. type(collection_name))
    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))

    local lookup_index_name = self.lookup_index_name
    assert(type(lookup_index_name) == 'table',
        'lookup_index_name must be a table, got ' .. type(lookup_index_name))

    local name_list = {}
    local value_list = {}
    local function fill_name_list(filter, base_name)
        for field_name, v in pairs(filter) do
            if type(v) == 'table' then
                fill_name_list(v, base_name .. field_name .. '.')
            else
                name_list[#name_list + 1] = base_name .. field_name
                value_list[#value_list + 1] = v
            end
        end
    end
    fill_name_list(filter, '')
    table.sort(name_list)
    local name_list_str = table.concat(name_list, ',')
    assert(lookup_index_name[collection_name] ~= nil,
        ('cannot find any index for collection "%s"'):format(collection_name))
    local index_name = lookup_index_name[collection_name][name_list_str]
    local full_match = index_name ~= nil
    return full_match, index_name, value_list
end

--- Build `lookup_index_name` table to use in the @{get_index_name} function.
--- @tparam table indexes map of from collection names to indexes as defined in
--- the @{new} function.
--- @treturn table `lookup_index_name`
local function build_lookup_index_name(indexes)
    assert(type(indexes) == 'table', 'indexes must be a table, got ' ..
        type(indexes))
    local lookup_index_name = {}
    for collection_name, index in pairs(indexes) do
        assert(type(collection_name) == 'string',
            'collection_name must be a string, got ' .. type(collection_name))
        assert(type(index) == 'table',
            'index must be a table, got ' .. type(index))

        lookup_index_name[collection_name] = {}

        for index_name, index_descr in pairs(index) do
            assert(type(index_name) == 'string',
                'index_name must be a string, got ' .. type(index_name))
            assert(type(index_descr) == 'table',
                'index_descr must be a table, got ' .. type(index_descr))
            assert(type(index_descr.service_fields) == 'table',
                'index_descr.service_fields must be a table, got ' ..
                type(index_descr.service_fields))
            assert(type(index_descr.fields) == 'table',
                'index_descr.fields must be a table, got ' ..
                type(index_descr.fields))

            local service_fields_list = index_descr['service_fields']
            assert(utils.is_array(service_fields_list),
                'service_fields_list must be an array')
            local fields_list = index_descr['fields']
            assert(utils.is_array(fields_list),
                'fields_list must be an array')
            assert(#service_fields_list + #fields_list > 0,
                'bad "indexes" parameter: no fields found')

            local name_list = {}
            for _, field in ipairs(service_fields_list) do
                assert(type(field) == 'string',
                    'field must be a string, got ' .. type(field))
                name_list[#name_list + 1] = field
            end
            for _, field in ipairs(fields_list) do
                assert(type(field) == 'string',
                    'field must be a string, got ' .. type(field))
                name_list[#name_list + 1] = field
            end

            table.sort(name_list)
            local name_list_str = table.concat(name_list, ',')
            lookup_index_name[collection_name][name_list_str] = index_name
        end
    end
    return lookup_index_name
end

--- Set of asserts to check the `opts.collections` argument of the
--- @{accessor_general.new} function.
--- @tparam table collections a map from collection names to collections as
--- defined in the @{accessor_general.new} function decription; this is subject
--- to validate
--- @tparam table schemas a map from schema names to schemas as defined in the
--- @{tarantool_graphql.new} function; this is for validate collection against
--- certain set of schemas (no 'dangling' schema names in collections)
--- @return nil
local function validate_collections(collections, schemas)
    for collection_name, collection in pairs(collections) do
        assert(type(collection_name) == 'string',
            'collection_name must be a string, got ' ..
            type(collection_name))
        assert(type(collection) == 'table',
            'collection must be a table, got ' .. type(collection))
        local schema_name = collection.schema_name
        assert(type(schema_name) == 'string',
            'collection.schema_name must be a string, got ' ..
            type(schema_name))
        assert(schemas[schema_name] ~= nil,
            ('cannot find schema "%s" for collection "%s"'):format(
            schema_name, collection_name))
        local connections = collection.connections
        assert(connections == nil or type(connections) == 'table',
            'collection.connections must be nil or table, got ' ..
            type(connections))
        for _, connection in ipairs(connections) do
            assert(type(connection) == 'table',
                'connection must be a table, got ' .. type(connection))
            assert(type(connection.name) == 'string',
                'connection.name must be a string, got ' ..
                type(connection.name))
            assert(type(connection.destination_collection) == 'string',
                'connection.destination_collection must be a string, got ' ..
                type(connection.destination_collection))
            assert(type(connection.parts) == 'table',
                'connection.parts must be a string, got ' ..
                type(connection.parts))
            for _, part in ipairs(connection.parts) do
                assert(type(part.source_field) == 'string',
                    'part.source_field must be a string, got ' ..
                    type(part.source_field))
                assert(type(part.destination_field) == 'string',
                    'part.destination_field must be a string, got ' ..
                    type(part.destination_field))
            end
        end
    end
end

--- The function is core of this module and implements logic of fetching and
--- filtering requested objects.
---
--- @tparam table self the data accessor created by the `new` function
--- (directly or indirectly using the `accessor_space.new` or the
--- `accessor_shard.new` function)
---
--- @tparam string collection_name name of collection to perform select
---
--- @tparam table from collection and connection names we arrive from/by or nil
--- as defined in the `tarantool_graphql.new` function description
---
--- @tparam table filter subset of object fields with values by which we want
--- to find full object(s)
---
--- @tparam table args table of arguments passed within the query except ones
--- that forms the `filter` parameter
---
--- @treturn table list of matching objects
local function select_internal(self, collection_name, from, filter, args)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' ..
        type(collection_name))
    assert(from == nil or type(from) == 'table',
        'from must be nil or a string, got ' .. type(from))
    assert(from == nil or type(from.collection_name) == 'string',
        'from must be nil or from.collection_name must be a string, got ' ..
        type((from or {}).collection_name))
    assert(from == nil or type(from.connection_name) == 'string',
        'from must be nil or from.connection_name must be a string, got ' ..
        type((from or {}).connection_name))

    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))
    assert(type(args) == 'table',
        'args must be a table, got ' .. type(args))
    assert(args.limit == nil or type(args.limit) == 'number',
        'args.limit must be a number of nil, got ' .. type(args.limit))
    assert(args.offset == nil or type(args.offset) == 'number',
        'args.offset must be a number of nil, got ' .. type(args.offset))

    local collection = self.collections[collection_name]
    assert(collection ~= nil,
        ('cannot find the collection "%s"'):format(
        collection_name))

    -- XXX: lookup index by connection_name, not filter?
    local full_match, index_name, index_value = get_index_name(self,
        collection_name, filter)
    assert(self.funcs.is_collection_exists(collection_name),
        ('cannot find collection "%s"'):format(collection_name))
    local index = self.funcs.get_index(collection_name, index_name)
    assert(index == nil or full_match == true,
        'only full index match is supported for now') -- XXX
    if from ~= nil then
        -- allow fullscan only for a top-level object
        assert(index ~= nil,
            ('cannot find index "%s" in space "%s"'):format(
            index_name, collection_name))
    end

    local schema_name = collection.schema_name
    assert(type(schema_name) == 'string',
        'schema_name must be a string, got ' .. type(schema_name))
    local model = self.models[schema_name]
    assert(model ~= nil,
        ('cannot find model for collection "%s"'):format(
        collection_name))

    -- XXX: this block becomes ugly after add support of filtering w/o index
    -- (using fullscan) for top-level objects; it need to be refactored
    local limit = args.limit
    local offset = args.offset
    local skipped = 0
    local count = 0
    local objs = {}
    local function process_tuple(tuple, do_filter)
        if offset ~= nil and skipped < offset then
            if do_filter then
                local ok, obj = model.unflatten(tuple)
                assert(ok, 'cannot unflat tuple: ' .. tostring(obj))
                local match = utils.is_subtable(obj, filter)
                if not match then return true end
            end
            skipped = skipped + 1
        else
            local ok, obj = model.unflatten(tuple)
            assert(ok, 'cannot unflat tuple: ' .. tostring(obj))
            local match = utils.is_subtable(obj, filter)
            if do_filter then
                if not match then return true end
            else
                assert(match, 'found object do not fit passed filter')
            end
            objs[#objs + 1] = obj
            count = count + 1
            if limit ~= nil and count >= limit then
                assert(limit == nil or count <= limit,
                    ('count[%d] exceeds limit[%s] (in for)'):format(
                    count, limit))
                return false
            end
        end
        return true
    end
    if index == nil then
        -- fullscan
        local primary_index = self.funcs.get_primary_index(collection_name)
        for _, tuple in primary_index:pairs() do
            if not process_tuple(tuple, not full_match) then break end
        end
    else
        for _, tuple in index:pairs(index_value) do
            if not process_tuple(tuple, not full_match) then break end
        end
    end
    assert(limit == nil or count <= limit,
        ('count[%d] exceeds limit[%s] (before return)'):format(count, limit))
    assert(#objs == count,
        ('count[%d] is not equal to objs count[%d]'):format(count, #objs))
    return objs
end

--- Set of asserts to check the `funcs` argument of the @{accessor_general.new}
--- function.
--- @tparam table funcs set of function as defined in the
--- @{accessor_general.new} function decription
--- @return nil
local function validate_funcs(funcs)
    assert(type(funcs) == 'table',
        'funcs must be a table, got ' .. type(funcs))
    assert(type(funcs.is_collection_exists) == 'function',
        'funcs.is_collection_exists must be a function, got ' ..
        type(funcs.is_collection_exists))
    assert(type(funcs.get_index) == 'function',
        'funcs.get_index must be a function, got ' ..
        type(funcs.get_index))
    assert(type(funcs.get_primary_index) == 'function',
        'funcs.get_primary_index must be a function, got ' ..
        type(funcs.get_primary_index))
end

--- Create a new data accessor.
---
--- Provided `funcs` argument determines certain functions for retrieving
--- tuples.
---
--- @tparam table opts `schemas`, `collections`, `service_fields` and `indexes`
--- to give the data accessor all needed meta-information re data; the format is
--- shown below
---
--- @tparam table funcs set of functions (`is_collection_exists`, `get_index`,
--- `get_primary_index`) allows this abstract data accessor behaves in the
--- certain way (say, like space data accessor or shard data accessor);
--- consider the `accessor_space` and the `accessor_shard` modules documentation
--- for this functions description
---
--- For examples of `opts.schemas` and `opts.collections` consider the
--- @{tarantool_graphql.new} function description.
---
--- Example of `opts.service_fields` item:
---
---     service_fields['schema_name'] = {
---         {name = 'expires_on', type = 'long', default = 0},
---     },
---
--- Example of `opts.indexes` item:
---
---     indexes['collection_name'] = {
---         foo_bar = {
---             service_fields = {},
---             fields = {'foo', 'bar'},
---         },
---         ...
---     }
---
--- @treturn table data accessor instance, a table with the two methods
--- (`select` and `arguments`) as described in the @{tarantool_graphql.new}
--- function description.
function accessor_general.new(opts, funcs)
    assert(type(opts) == 'table',
        'opts must be a table, got ' .. type(opts))
    assert(type(funcs) == 'table',
        'funcs must be a table, got ' .. type(funcs))

    local schemas = opts.schemas
    local collections = opts.collections
    local service_fields = opts.service_fields
    local indexes = opts.indexes

    assert(type(schemas) == 'table',
        'schemas must be a table, got ' .. type(schemas))
    assert(type(collections) == 'table',
        'collections must be a table, got ' .. type(collections))
    assert(type(service_fields) == 'table',
        'service_fields must be a table, got ' .. type(service_fields))
    assert(type(indexes) == 'table',
        'indexes must be a table, got ' .. type(indexes))

    local models = compile_schemas(schemas, service_fields)
    validate_collections(collections, schemas)
    local lookup_index_name = build_lookup_index_name(indexes)

    validate_funcs(funcs)

    return setmetatable({
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
        models = models,
        lookup_index_name = lookup_index_name,
        funcs = funcs,
    }, {
        __index = {
            select = function(self, parent, collection_name, from,
                    filter, args)
                assert(type(parent) == 'table',
                    'parent must be a table, got ' .. type(parent))
                assert(from == nil or type(from) == 'table',
                    'from must be nil or a string, got ' .. type(from))
                assert(from == nil or type(from.collection_name) == 'string',
                    'from must be nil or from.collection_name ' ..
                    'must be a string, got ' ..
                    type((from or {}).collection_name))
                assert(from == nil or type(from.connection_name) == 'string',
                    'from must be nil or from.connection_name ' ..
                    'must be a string, got ' ..
                    type((from or {}).connection_name))
                return select_internal(self, collection_name, from, filter,
                    args)
            end,
            arguments = function(self, connection_type)
                if connection_type == '1:1' then return {} end
                return {
                    {name = 'limit', type = 'int'},
                    {name = 'offset', type = 'long'},
                    -- {name = 'filter', type = ...},
                }
            end,
        }
    })
end

return accessor_general

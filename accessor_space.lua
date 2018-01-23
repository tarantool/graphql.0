local avro_schema = require('avro_schema')
local utils = require('utils')

local accessor_space = {}

--[[
    local service_fields_types = {}
    for i, v in ipairs(service_fields) do
        service_fields_types[#service_fields_types + 1] = v.type
    end
]]--

-- XXX: different service_fields for different collections
local function compile_schemas(schemas, service_fields)
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

        local ok, handle = avro_schema.create(schema, {}) -- XXX: service_fields
        assert(ok, ('cannot create avro_schema for %s: %s'):format(
            name, tostring(handle)))
        local ok, model = avro_schema.compile(handle)
        assert(ok, ('cannot compile avro_schema for %s: %s'):format(
            name, tostring(model)))

        models[name] = model
    end
    return models
end

local get_index_name = function(collection_name, filter, lookup_index_name)
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' .. type(collection_name))
    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))
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
    return index_name, value_list
end

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
            -- XXX: assert for index_descr fields

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

local function select_internal(self, collection_name, filter, args)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a table, got ' ..
        type(collection_name))
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
        ('cannot found the collection "%s"'):format(
        collection_name))

    local index_name, index_value = get_index_name(
        collection_name, filter, self.lookup_index_name)
    assert(box.space[collection_name] ~= nil,
        ('cannot find space "%s"'):format(collection_name))
    local index = box.space[collection_name].index[index_name]
    assert(index ~= nil,
        ('cannot find index "%s" in space "%s"'):format(
        index_name, collection_name))

    local schema_name = collection.schema_name
    assert(type(schema_name) == 'string',
        'schema_name must be a string, got ' .. type(schema_name))
    local model = self.models[schema_name]
    assert(model ~= nil,
        ('cannot find model for collection "%s"'):format(
        collection_name))

    local limit = args.limit
    local offset = args.offset
    local skipped = 0
    local count = 0
    local objs = {}
    for _, tuple in index:pairs(index_value) do
        if offset ~= nil and skipped < offset then
            skipped = skipped + 1
        else
            local ok, obj = model.unflatten(tuple)
            assert(ok, 'cannot unflat tuple: ' .. tostring(obj))
            assert(utils.is_subtable(obj, filter),
                'found object do not fit passed filter')
            objs[#objs + 1] = obj
            count = count + 1
            if limit ~= nil and count >= limit then
                assert(limit == nil or count <= limit,
                    ('count[%d] exceeds limit[%s] (in for)'):format(
                    count, limit))
                break
            end
        end
    end
    assert(limit == nil or count <= limit,
        ('count[%d] exceeds limit[%s] (before return)'):format(count, limit))
    assert(#objs == count,
        ('count[%d] is not equal to objs count[%d]'):format(count, #objs))
    return objs
end

--- Example of service_fields item:
---
---     service_fields['collection_name'] = {
---         {name = 'expires_on', type = 'long', default = 0},
---     },
---
--- Example of indexes item:
---
---     indexes['collection_name'] = {
---         foo_bar = {
---             service_fields = {},
---             fields = {'foo', 'bar'},
---             index_type = 'tree',
---             unique = true,
---         },
---         ...
---     }
function accessor_space.new(opts)
    assert(type(opts) == 'table',
        'opts must be a table, got ' .. type(opts))

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
    -- XXX: validate collections
    -- - key must be a string
    -- - .schema_name must be a string
    -- XXX: validate service_fields
    -- - key must be a string
    local lookup_index_name = build_lookup_index_name(indexes)

    -- XXX: we need to pass a connection name in case there are different
    --      connections with the same fields in a different order
    return setmetatable({
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
        models = models,
        lookup_index_name = lookup_index_name,
    }, {
        __index = {
            get = function(self, parent, collection_name, filter, args)
                assert(type(parent) == 'table',
                    'parent must be a table, got ' .. type(parent))
                assert(next(args) == nil,
                    'non-empty args list for get request')
                local objs = select_internal(self, collection_name, filter, args)
                assert(#objs == 1, 'expect one matching object, got ' ..
                    tostring(#objs))
                return objs[1]
            end,
            select = function(self, parent, collection_name, filter, args)
                assert(type(parent) == 'table',
                    'parent must be a table, got ' .. type(parent))
                local objs = select_internal(self, collection_name, filter, args)
                return objs
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

return accessor_space

--- Abstraction layer between a data collections (e.g. tarantool's spaces) and
--- the GraphQL query language.

local json = require('json')

local parse = require('graphql.parse')
local schema = require('graphql.schema')
local types = require('graphql.types')
local validate = require('graphql.validate')
local execute = require('graphql.execute')

local tarantool_graphql = {}

-- forward declarations
local gql_type

--- Check whether table is an array.
--- Based on [that][1].
--- [1]: https://github.com/mpx/lua-cjson/blob/db122676/lua/cjson/util.lua
--- @param table
--- @return True if table is an array
local function is_array(table)
    if type(table) ~= 'table' then
        return false
    end
    local max = 0
    local count = 0
    for k, _ in pairs(table) do
        if type(k) == 'number' then
            if k > max then
                max = k
            end
            count = count + 1
        else
            return false
        end
    end
    if max > count * 2 then
        return false
    end

    return max >= 0
end

local function avro_type(avro_schema)
    if type(avro_schema) == 'table' and avro_schema.type == 'record' then
        return 'record'
    elseif type(avro_schema) == 'table' and is_array(avro_schema) then
        return 'enum'
    elseif type(avro_schema) == 'string' and avro_schema == 'int' then
        return 'int'
    elseif type(avro_schema) == 'string' and avro_schema == 'long' then
        return 'long'
    elseif type(avro_schema) == 'string' and avro_schema == 'string' then
        return 'string'
    else
        error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
    end
end

-- XXX: recursive skip several NonNull's?
local function nullable(gql_class)
    assert(type(gql_class) == 'table', 'gql_class must be a table, got ' ..
        type(gql_class))

    if gql_class.__type ~= 'NonNull' then return gql_class end

    assert(gql_class.ofType ~= nil, 'gql_class.ofType must not be nil')
    return gql_class.ofType
end

--- Convert each field of an avro-schema to a graphql type and corresponding
--- argument for an upper graphql type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types
--- @tparam table fields fields part from an avro-schema
local function convert_record_fields(state, fields)
    local res = {}
    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))
        res[field.name] = {
            name = field.name,
            kind = gql_type(state, field.type),
        }
        args[field.name] = nullable(res[field.name].kind)
    end
    return res, args
end

local types_long = types.scalar({
    name = 'Long',
    description = 'Long is non-bounded integral type',
    serialize = function(value) return tonumber(value) end,
    parseValue = function(value) return tonumber(value) end,
    parseLiteral = function(node)
        if node.kind == 'long' then -- XXX: from where this 'long' we can get?
            return tonumber(node.value)
        end
    end
})

--- The function recursively converts passed avro-schema to a graphql type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types
--- @tparam table avro_schema input avro-schema
--- @tparam[opt] table collection table with schema_name, connections fields
--- described a collection (e.g. tarantool's spaces)
---
--- If collection is passed, two things are changed within this function:
---
--- 1. Connections from the collection will be taken into account to
---    automatically generate corresponding decucible fields.
--- 2. The collection name will be used as the resulting graphql type name
---    instead of the avro-schema name.
gql_type = function(state, avro_schema, collection)
    local state = state or {}
    assert(type(state) == 'table',
        'state must be a table or nil, got ' .. type(state))
    local accessor = state.accessor
    assert(accessor ~= nil, 'state.accessor must not be nil')
    assert(accessor.get ~= nil, 'state.accessor.get must not be nil')
    assert(accessor.select ~= nil, 'state.accessor.select must not be nil')

    if avro_type(avro_schema) == 'record' then
        assert(type(avro_schema.name) == 'string',
            ('avro_schema.name must be a string, got %s (avro_schema %s)')
            :format(type(avro_schema.name), json.encode(avro_schema)))
        assert(type(avro_schema.fields) == 'table',
            ('avro_schema.fields must be a table, got %s (avro_schema %s)')
            :format(type(avro_schema.fields), json.encode(avro_schema)))

        local fields, args = convert_record_fields(state,
            avro_schema.fields)

        -- XXX: check connection.destination_objects (single / many) to let
        --      type be an object or list (notNull list!);
        -- XXX: custom limiting/filtering arguments (limit, offset, filter) in
        --      case of 1:N connection
        for _, c in ipairs((collection or {}).connections or {}) do
            assert(type(c.name) == 'string',
                'connection.name must be a string, got ' .. type(c.name))
            assert(type(c.destination_collection) == 'string',
                'connection.destination_collection must be a string, got ' ..
                type(c.destination_collection))
            assert(type(c.parts) == 'table',
                'connection.parts must be a string, got ' .. type(c.parts))

            local destination_type =
                state.types[c.destination_collection]
            assert(destination_type ~= nil,
                ('destination_type (named %s) must not be nil'):format(
                c.destination_collection))
            fields[c.name] = {
                name = c.name,
                kind = destination_type,
                resolve = function(parent, args, info)
                    local args = table.copy(args) -- luacheck: ignore
                    -- XXX: pass args for accessor like so:
                    --      {parent_fields = ..., args = ...}
                    for _, part in ipairs(c.parts) do
                        assert(type(part.source_field) == 'string',
                            'part.source_field must be a string, got ' ..
                            type(part.destination_field))
                        assert(type(part.destination_field) == 'string',
                            'part.destination_field must be a string, got ' ..
                            type(part.destination_field))
                        args[part.destination_field] =
                            parent[part.source_field]
                    end
                    return accessor:get(parent, c.destination_collection, args)
                end,
            }
        end

        -- XXX: use connection name as name of the field, not collection name
        local res = types.nonNull(types.object({
            name = collection ~= nil and collection.name or avro_schema.name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        }))

       return res, args, avro_schema.name
    elseif avro_type(avro_schema) == 'enum' then
        error('enums not implemented yet') -- XXX
    elseif avro_type(avro_schema) == 'int' then
        return types.int.nonNull
    elseif avro_type(avro_schema) == 'long' then
        return types_long.nonNull
    elseif avro_type(avro_schema) == 'string' then
        return types.string.nonNull
    else
        error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
    end
end

--- Generate an object that behaves like a table stores another tables as
--- values and always returns the same table (the same reference) as a value.
--- It performs copying of a value fields instead of assigning and returns an
--- empty table for fields that not yet exists. Such approach helps with
--- referencing a table that will be filled later.
---
--- @tparam table data the initial values
local function gen_booking_table(data)
    assert(type(data) == 'table',
        'initial data must be a table, got ' .. type(data))
    return setmetatable({data = data}, {
        __index = function(table, key)
            local data = rawget(table, 'data')
            if data[key] == nil then
                data[key] = {}
            end
            return data[key]
        end,
        __newindex = function(table, key, value)
            assert(type(value) == 'table',
                'value to set must be a table, got ' .. type(value))
            local data = rawget(table, 'data')
            if data[key] == nil then
                data[key] = {}
            end
            for k, _ in pairs(data[key]) do
                data[key][k] = nil
            end
            assert(next(data[key]) == nil,
                ('data[%s] must be nil, got %s'):format(tostring(key),
                tostring(next(data[key]))))
            for k, v in pairs(value) do
                data[key][k] = v
            end
        end,
    })
end

local function parse_cfg(cfg)
    local state = {}
    state.types = gen_booking_table({})
    state.arguments = {}

    local accessor = cfg.accessor
    assert(accessor ~= nil, 'cfg.accessor must not be nil')
    assert(accessor.get ~= nil, 'cfg.accessor.get must not be nil')
    assert(accessor.select ~= nil, 'cfg.accessor.select must not be nil')
    state.accessor = accessor

    assert(cfg.collections ~= nil, 'cfg.collections must not be nil')
    local collections = table.copy(cfg.collections) -- luacheck: ignore
    state.collections = collections

    local fields = {}

    for name, collection in pairs(state.collections) do
        collection.name = name
        assert(collection.schema_name ~= nil,
            'collection.schema_name must not be nil')
        local schema = cfg.schemas[collection.schema_name]
        assert(schema ~= nil, ('cfg.schemas[%s] must not be nil'):format(
            tostring(collection.schema_name)))
        local schema_name
        state.types[name], state.arguments[name], schema_name =
            gql_type(state, schema, collection)
        assert(schema_name == nil or schema_name == collection.schema_name,
            ('top-level schema name does not match the name in ' ..
            'the schema itself: "%s" vs "%s"'):format(collection.schema_name,
            schema_name))

        -- create entry points from collection names
        fields[name] = {
            kind = types.nonNull(types.list(state.types[name])),
            arguments = state.arguments[name],
            resolve = function(rootValue, args, info)
                return accessor:select(rootValue, name, args)
            end,
        }
    end

    local schema = schema.create({
        query = types.object({
            name = 'Query',
            fields = fields,
        })
    })
    state.schema = schema

    return state
end

local function assert_gql_query_ast(func_name, ast)
    assert(#ast.definitions == 1,
        func_name .. ': expected an one query')
    assert(ast.definitions[1].operation == 'query',
        func_name .. ': expected a query operation')
    local operation_name = ast.definitions[1].name.value
    assert(type(operation_name) == 'string',
        func_name .. 'operation_name must be a string, got ' ..
        type(operation_name))
end

local function gql_execute(qstate, variables)
    assert(qstate.state)
    local state = qstate.state
    assert(state.schema)

    assert(type(variables) == 'table', 'variables must be table, got ' ..
        type(variables))

    local root_value = {}
    local operation_name = qstate.operation_name
    assert(type(operation_name) == 'string',
        'operation_name must be a string, got ' .. type(operation_name))

    return execute(state.schema, qstate.ast, root_value, variables,
        operation_name)
end

local function gql_compile(state, query)
    assert(type(state) == 'table' and type(query) == 'string',
        'use :validate(...) instead of .validate(...)')
    assert(state.schema ~= nil, 'have not compiled schema')

    local ast = parse(query)
    assert_gql_query_ast('gql_compile', ast)
    local operation_name = ast.definitions[1].name.value

    validate(state.schema, ast)

    local qstate = {
        state = state,
        ast = ast,
        operation_name = operation_name,
    }
    local gql_query = setmetatable(qstate, {
        __index = {
            execute = gql_execute,
        }
    })
    return gql_query
end

--- Create a tarantool_graphql library instance.
---
--- Usage:
---
--- ... = tarantool_graphql.new({
---     schemas = {
---         schema_name_foo = { // the value is avro-schema (esp., a record)
---             name = 'schema_name_foo,
---             type = 'record',
---             fields = {
---                 ...
---             }
---         },
---         ...
---     },
---     collections = {
---         collections_name_foo = {
---             type = 'schema_name_foo',
---             connections = { // the optional field
---                 {
---                     name = 'connection_name_bar',
---                     destination_collection = 'collection_baz',
---                     parts = {
---                         {
---                             source_field = 'field_name_source_1',
---                             destination_field = 'field_name_destination_1'
---                         }
---                     }
---                 },
---                 ...
---             },
---         },
---         ...
---     },
---     accessor = setmetatable({}, {
---         __index = {
---             get = function(self, parent, name, args)
---                 return ...
---             end,
---             select = function(self, parent, name, args)
---                 return ...
---             end,
---         }
---     }),
--- })
function tarantool_graphql.new(cfg)
    local state = parse_cfg(cfg)
    return setmetatable(state, {
        __index = {
            compile = gql_compile,
        }
    })
end

return tarantool_graphql

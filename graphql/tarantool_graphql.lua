--- Abstraction layer between a data collections (e.g. tarantool's spaces) and
--- the GraphQL query language.
---
--- Random notes:
---
--- * GraphQL top level statement must be a collection name. Arguments for this
---   statement match non-deducible field names of corresponding object and
---   passed to an accessor function in the filter argument.

local json = require('json')

local parse = require('graphql.core.parse')
local schema = require('graphql.core.schema')
local types = require('graphql.core.types')
local validate = require('graphql.core.validate')
local execute = require('graphql.core.execute')

local utils = require('graphql.utils')

local tarantool_graphql = {}

-- forward declarations
local gql_type

local function avro_type(avro_schema)
    if type(avro_schema) == 'table' and avro_schema.type == 'record' then
        return 'record'
    elseif type(avro_schema) == 'table' and utils.is_array(avro_schema) then
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

local types_long = types.scalar({
    name = 'Long',
    description = 'Long is non-bounded integral type',
    serialize = function(value) return tonumber(value) end,
    parseValue = function(value) return tonumber(value) end,
    parseLiteral = function(node)
        if node.kind == 'int' then
            return tonumber(node.value)
        end
    end
})

-- XXX: boolean
-- XXX: float
local function convert_scalar_type(avro_schema, opts)
    local opts = opts or {}
    assert(type(opts) == 'table', 'opts must be nil or table, got ' ..
        type(opts))
    local raise = opts.raise or false
    assert(type(opts.raise) == 'boolean', 'opts.raise must be boolean, got ' ..
        type(opts.raise))

    local avro_t = avro_type(avro_schema)
    if avro_t == 'int' then
        return types.int.nonNull
    elseif avro_t == 'long' then
        return types_long.nonNull
    elseif avro_t == 'string' then
        return types.string.nonNull
    end
    if raise then
        error('unrecognized avro-schema scalar type: ' ..
            json.encode(avro_schema))
    end
    return nil
end

local function convert_scalar_record_fields_to_args(fields)
    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))
        local gql_class = convert_scalar_type(field.type, {raise = true})
        args[field.name] = nullable(gql_class)
    end
    return args
end

--- Convert each field of an avro-schema to a graphql type and corresponding
--- argument for an upper graphql type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types
--- @tparam table fields fields part from an avro-schema
local function convert_record_fields(state, fields)
    local res = {}
    local object_args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))
        res[field.name] = {
            name = field.name,
            kind = gql_type(state, field.type),
        }
        object_args[field.name] = nullable(res[field.name].kind)
    end
    return res, object_args
end

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
gql_type = function(state, avro_schema, collection, collection_name)
    local state = state or {}
    assert(type(state) == 'table',
        'state must be a table or nil, got ' .. type(state))
    assert(collection == nil or type(collection) == 'table',
        'collection must be nil or a table, got ' .. type(collection))
    assert(collection_name == nil or type(collection_name) == 'string',
        'collection_name must be nil or a string, got ' ..
        type(collection_name))
    assert((collection == nil and collection_name == nil) or
        (collection ~= nil and collection_name ~= nil),
        ('collection and collection_name must be nils or ' ..
        'non-nils simultaneously, got: %s and %s'):format(type(collection),
        type(collection_name)))
    local accessor = state.accessor
    assert(accessor ~= nil, 'state.accessor must not be nil')
    assert(accessor.select ~= nil, 'state.accessor.select must not be nil')
    assert(accessor.list_args ~= nil,
        'state.accessor.list_args must not be nil')

    if avro_type(avro_schema) == 'record' then
        assert(type(avro_schema.name) == 'string',
            ('avro_schema.name must be a string, got %s (avro_schema %s)')
            :format(type(avro_schema.name), json.encode(avro_schema)))
        assert(type(avro_schema.fields) == 'table',
            ('avro_schema.fields must be a table, got %s (avro_schema %s)')
            :format(type(avro_schema.fields), json.encode(avro_schema)))

        local fields, _ = convert_record_fields(state, avro_schema.fields)

        for _, c in ipairs((collection or {}).connections or {}) do
            assert(type(c.type) == 'string',
                'connection.type must be a string, got ' .. type(c.type))
            assert(c.type == '1:1' or c.type == '1:N',
                'connection.type must be 1:1 or 1:N, got ' .. c.type)
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

            local c_args
            if c.type == '1:1' then
                c_args = state.object_arguments[c.destination_collection]
            elseif c.type == '1:N' then
                destination_type = types.nonNull(types.list(destination_type))
                c_args = state.all_arguments[c.destination_collection]
            else
                error('unknown connection type: ' .. tostring(c.type))
            end

            local c_list_args = state.list_arguments[c.destination_collection]

            fields[c.name] = {
                name = c.name,
                kind = destination_type,
                arguments = c_args,
                resolve = function(parent, args_instance, info)
                    local destination_args_names = {}
                    local destination_args_values = {}
                    for _, part in ipairs(c.parts) do
                        assert(type(part.source_field) == 'string',
                            'part.source_field must be a string, got ' ..
                            type(part.destination_field))
                        assert(type(part.destination_field) == 'string',
                            'part.destination_field must be a string, got ' ..
                            type(part.destination_field))
                        destination_args_names[#destination_args_names + 1] =
                            part.destination_field
                        destination_args_values[#destination_args_values + 1] =
                            parent[part.source_field]
                    end
                    local from = {
                        collection_name = collection_name,
                        connection_name = c.name,
                        destination_args_names = destination_args_names,
                        destination_args_values = destination_args_values,
                    }

                    local object_args_instance = {} -- passed to 'filter'
                    local list_args_instance = {} -- passed to 'args'
                    for k, v in pairs(args_instance) do
                        if c_list_args[k] ~= nil then
                            list_args_instance[k] = v
                        elseif c_args[k] ~= nil then
                            object_args_instance[k] = v
                        else
                            error(('cannot found "%s" field ("%s" value) ' ..
                                'within allowed fields'):format(tostring(k),
                                tostring(v)))
                        end
                    end
                    local objs = accessor:select(parent,
                        c.destination_collection, from,
                        object_args_instance, list_args_instance)
                    assert(type(objs) == 'table',
                        'objs list received from an accessor ' ..
                        'must be a table, got ' .. type(objs))
                    if c.type == '1:1' then
                        assert(#objs == 1,
                            'expect one matching object, got ' ..
                            tostring(#objs))
                        return objs[1]
                    else -- c.type == '1:N'
                        return objs
                    end
                end,
            }
        end

        local res = types.nonNull(types.object({
            name = collection ~= nil and collection.name or avro_schema.name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        }))

        return res
    elseif avro_type(avro_schema) == 'enum' then
        error('enums not implemented yet') -- XXX
    else
        local res = convert_scalar_type(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
        end
        return res
    end
end

local function parse_cfg(cfg)
    local state = {}
    state.types = utils.gen_booking_table({})
    state.object_arguments = utils.gen_booking_table({})
    state.list_arguments = utils.gen_booking_table({})
    state.all_arguments = utils.gen_booking_table({})

    local accessor = cfg.accessor
    assert(accessor ~= nil, 'cfg.accessor must not be nil')
    assert(accessor.select ~= nil, 'cfg.accessor.select must not be nil')
    assert(accessor.list_args ~= nil,
        'state.accessor.list_args must not be nil')
    state.accessor = accessor

    assert(cfg.collections ~= nil, 'cfg.collections must not be nil')
    local collections = table.copy(cfg.collections)
    state.collections = collections

    local fields = {}

    for name, collection in pairs(state.collections) do
        collection.name = name
        assert(collection.schema_name ~= nil,
            'collection.schema_name must not be nil')
        local schema = cfg.schemas[collection.schema_name]
        assert(schema ~= nil, ('cfg.schemas[%s] must not be nil'):format(
            tostring(collection.schema_name)))
        assert(schema.name == nil or schema.name == collection.schema_name,
            ('top-level schema name does not match the name in ' ..
            'the schema itself: "%s" vs "%s"'):format(collection.schema_name,
            schema.name))
        state.types[name] = gql_type(state, schema, collection, name)

        local _, object_args = convert_record_fields(state,
            schema.fields)
        -- XXX: support InputObject (non-scalar types in arguments)
        local list_args = convert_scalar_record_fields_to_args(
            accessor:list_args(name))
        local args = utils.merge_tables(object_args, list_args)
        state.object_arguments[name] = object_args
        state.list_arguments[name] = list_args
        state.all_arguments[name] = args

        -- create entry points from collection names
        fields[name] = {
            kind = types.nonNull(types.list(state.types[name])),
            arguments = state.all_arguments[name],
            resolve = function(rootValue, args_instance, info)
                local object_args_instance = {} -- passed to 'filter'
                local list_args_instance = {} -- passed to 'args'
                for k, v in pairs(args_instance) do
                    if list_args[k] ~= nil then
                        list_args_instance[k] = v
                    elseif state.object_arguments[k] ~= nil then
                        object_args_instance[k] = v
                    else
                        error(('cannot found "%s" field ("%s" value) ' ..
                            'within allowed fields'):format(tostring(k),
                            tostring(v)))
                    end
                end
                local from = nil
                return accessor:select(rootValue, name, from,
                    object_args_instance, list_args_instance)
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
---             schema_name = 'schema_name_foo',
---             connections = { // the optional field
---                 {
---                     name = 'connection_name_bar',
---                     destination_collection = 'collection_baz',
---                     parts = {
---                         {
---                             source_field = 'field_name_source_1',
---                             destination_field = 'field_name_destination_1'
---                         },
---                         ...
---                     },
---                     index_name = 'index_name' -- is is for an accessor,
---                                               -- ignored in the graphql
---                                               -- part
---                 },
---                 ...
---             },
---         },
---         ...
---     },
---     accessor = setmetatable({}, {
---         __index = {
---             select = function(self, parent, collection_name, from,
---                     object_args_instance, list_args_instance)
---                 -- from is nil for a top-level object, otherwise it is
---                 --
---                 -- {
---                 --     collection_name = <...>,
---                 --     connection_name = <...>,
---                 --     destination_args_names = <...>,
---                 --     destination_args_values = <...>,
---                 -- }
---                 --
---                 return ...
---             end,
---             list_args = function(self, collection_name)
---                 return {
---                     {name = 'limit', type = 'int'},
---                     {name = 'offset', type = <...>}, -- type of primary key
---                 }
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

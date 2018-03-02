--- Abstraction layer between a data collections (e.g. tarantool's spaces) and
--- the GraphQL query language.
---
--- Random notes:
---
--- * GraphQL top level statement must be a collection name. Arguments for this
---   statement match non-deducible field names of corresponding object and
---   passed to an accessor function in the filter argument.

local json = require('json')
local yaml = require('yaml')

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
    if type(avro_schema) == 'table' then
        if avro_schema.type == 'record' then
            return 'record'
        elseif avro_schema.type == 'record*' then
            return 'record*'
        elseif utils.is_array(avro_schema) then
            return 'union'
        elseif avro_schema.type == 'array' then
            return 'array'
        elseif avro_schema.type == 'array*' then
            return 'array*'
        elseif avro_schema.type == 'map' then
            return 'map'
        elseif avro_schema.type == 'map*' then
            return 'map*'
        end
    elseif type(avro_schema) == 'string' then
        if avro_schema == 'int' then
            return 'int'
        elseif avro_schema == 'int*' then
            return 'int*'
        elseif avro_schema == 'long' then
            return 'long'
        elseif avro_schema == 'long*' then
            return 'long*'
        elseif avro_schema == 'string' then
            return 'string'
        elseif avro_schema == 'string*' then
            return 'string*'
        end
    end
    error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
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

local types_map = types.scalar({
    name = 'Map',
    description = 'Map is a dictionary with string keys and values of ' ..
        'arbitrary but same among all values type',
    serialize = function(value) return value end,
    parseValue = function(value) return value end,
    parseLiteral = function(node)
        if node.kind == 'Map' then
            return node.value
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
    assert(type(raise) == 'boolean', 'opts.raise must be boolean, got ' ..
        type(raise))

    local avro_t = avro_type(avro_schema)
    if avro_t == 'int' then
        return types.int.nonNull
    elseif avro_t == 'int*' then
        return types.int
    elseif avro_t == 'long' then
        return types_long.nonNull
    elseif avro_t == 'long*' then
        return types_long
    elseif avro_t == 'string' then
        return types.string.nonNull
    elseif avro_t == 'string*' then
        return types.string
    end

    if raise then
        error('unrecognized avro-schema scalar type: ' ..
            json.encode(avro_schema))
    end

    return nil
end

--- Non-recursive version of the @{gql_type} function that returns
--- InputObject (instead of Object) or a scalar type.
---
--- An error will be raised if avro_schema type is 'record'
--- and its' fields have non-scalar types. So triple nesting level is not
--- supported (record with record as a field - ok, record with record which
--- has inside an another level - not ok).
local function gql_argument_type(avro_schema)
    assert(avro_schema ~= nil,
        'avro_schema must not be nil')

    if avro_type(avro_schema) == 'record' then
        assert(type(avro_schema.name) == 'string',
            ('avro_schema.name must be a string, got %s (avro_schema %s)')
            :format(type(avro_schema.name), json.encode(avro_schema)))

        assert(type(avro_schema.fields) == 'table',
            ('avro_schema.fields must be a table, got %s (avro_schema %s)')
            :format(type(avro_schema.fields), json.encode(avro_schema)))

        local fields = {}
        for _, field in ipairs(avro_schema.fields) do
            assert(type(field.name) == 'string',
                ('field.name must be a string, got %s (schema %s)')
                :format(type(field.name), json.encode(field)))

            local gql_field_type = convert_scalar_type(
                field.type, {raise = true})

            fields[field.name] = {
                name = field.name,
                kind = types.nonNull(gql_field_type),
            }
        end

        local res = types.nonNull(types.inputObject({
            name = avro_schema.name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        }))

        return res
    else
        local res = convert_scalar_type(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' ..
                json.encode(avro_schema))
        end
        return res
    end
end

--- Convert each field of an avro-schema to a scalar graphql type or an input
--- object.
---
--- It uses the @{gql_argument_type} function to convert each field, then skips
--- fields of record, array and map types and gives the resulting list of
--- converted fields.
---
--- @tparam table fields list of fields of the avro-schema record fields format
---
--- @tparam[opt] table opts optional options:
---
--- * `skip_compound` -- do not add fields of record type to the arguments;
--- default: false.
---
--- @treturn table `args` -- map with type names as keys and graphql types as
--- values
local function convert_record_fields_to_args(fields, opts)
    assert(type(fields) == 'table',
        'fields must be a table, got ' .. type(fields))

    local opts = opts or {}
    assert(type(opts) == 'table',
        'opts must be a table, got ' .. type(opts))

    local skip_compound = opts.skip_compound or false
    assert(type(skip_compound) == 'boolean',
        'skip_compound must be a boolean, got ' .. type(skip_compound))

    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))

        -- records, arrays (gql lists) and maps can't be arguments, so these
        -- graphql types are to be skipped
        local avro_t = avro_type(field.type)
        if not skip_compound or (
                avro_t ~= 'record' and avro_t ~= 'record*' and
                avro_t ~= 'array' and avro_t ~= 'array*' and
                avro_t ~= 'map' and avro_t ~= 'map*') then
            local gql_class = gql_argument_type(field.type)
            args[field.name] = nullable(gql_class)
        end
    end
    return args
end

--- Convert each field of an avro-schema to a graphql type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types
--- @tparam table fields fields part from an avro-schema
---
--- @treturn table `res` -- map with type names as keys and graphql types as
--- values
local function convert_record_fields(state, fields)
    local res = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))

        res[field.name] = {
            name = field.name,
            kind = gql_type(state, field.type),
        }
    end
    return res
end

--@todo where to put new format description?

--- The function converts passed simple connection to a field of GraphQL type
--- There are two types on connections: simple and union

---
--- @tparam table state for read state.accessor and previously filled
--- state.types (state.types are gql types)
--- @tparam table c simple connection to create field on
--- @tparam table collection_name name of the collection which have given
--- connection
local convert_simple_connection = function(state, c, collection_name)
    assert(type(c.destination_collection) == 'string',
    'connection.destination_collection must be a string, got ' ..
    type(c.destination_collection))
    assert(type(c.parts) == 'table',
    'connection.parts must be a string, got ' .. type(c.parts))

    -- gql type of connection field
    local destination_type = state.types[c.destination_collection]
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

    local field = {
        name = c.name,
        kind = destination_type,
        arguments = c_args,
        resolve = function(parent, args_instance, info)
            --print('print from 295 - resolve of simple connection')
            --print('parent - resolve result from parent type')
            --require('pl.pretty').dump(parent)
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
            local extra = {
                qcontext = info.qcontext
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
            local objs = state.accessor:select(parent,
            c.destination_collection, from,
            object_args_instance, list_args_instance, extra)
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

    return field
end

local convert_union_connection = function(state, c, collection_name)
    local union_types = {}
    local collection_to_arguments = {}
    local collection_to_list_arguments = {}
    -- map from determinant objects to use in resolveType
    -- not to use like determinant[determinant_value] = ...
    -- use like for k, v in pairs() ...
    -- {{hero_type = 'human', number_of_legs = '2'} = 'human_collection', {
    local determinant_keys = utils.get_keys(c.variants[1].determinant)
    local determinant_to_variant = {}

    for _, v in ipairs(c.variants) do
        assert(v.determinant, 'each variant should have a determinant')
        assert(type(v.determinant) == 'table', 'variant\'s determinant must ' ..
        'end be a table, got ' .. type(v.determinant))
        assert(type(v.destination_collection) == 'string',
            'variant.destination_collection must be a string, got ' ..
            type(v.destination_collection))
        assert(type(v.parts) == 'table',
        'variant.parts must be a string, got ' .. type(v.parts))
        local destination_type = state.types[v.destination_collection]
        assert(destination_type ~= nil,
            ('destination_type (named %s) must not be nil'):format(
            v.destination_collection))

        determinant_to_variant[v.determinant] = v

        local v_args
        if c.type == '1:1' then
            v_args = state.object_arguments[v.destination_collection]
        elseif c.type == '1:N' then
            destination_type = types.nonNull(types.list(destination_type))
            v_args = state.all_arguments[v.destination_collection]
        end

        local v_list_args = state.list_arguments[v.destination_collection]

        union_types[#union_types + 1] = destination_type

        collection_to_arguments[v.destination_collection] = v_args
        collection_to_list_arguments[v.destination_collection] = v_list_args
    end

    -- should return graphQL type (collection in our terms)
    local function resolveType(result)
        --@todo fix this as it will work only for human-starship union
        if utils.do_have_keys(result, {'name'}) then
            return state.types['human_collection']
        end

        if utils.do_have_keys(result, {'model'}) then
            return state.types['starship_collection']
        end
    end

    local function resolve_variant(parent)
        assert(utils.do_have_keys(parent, determinant_keys),
            ('Parent object of union object doesn\'t have determinant fields' ..
                'which are nessesary to determine which resolving variant should' ..
                'be used. Union parent object:\n"%s"\n Determinant keys:\n"%s"'):
            format(yaml.encode(parent), yaml.encode(determinant_keys)))

        local resulting_variant
        for determinant, variant in pairs(determinant_to_variant) do
            local is_match = true
            for determinant_key, determinant_value in pairs(determinant) do
                if parent[determinant_key] ~= determinant_value then
                    is_match = false
                    break
                end
            end

            if is_match then
                resulting_variant = variant
                break
            end
        end

        assert(resulting_variant, ('Variant resolving failed.'..
            'Parent object: "%s"\n'):format(yaml.encode(parent)))
        return resulting_variant
    end

    local field = {
        name = c.name,
        kind = types.union({name = c.name, types = union_types, resolveType = resolveType}),
        arguments =  nil,
        resolve = function(parent, args_instance, info)
            --variant for this destination
            local v = resolve_variant(parent)
            local destination_collection = state.types[v.destination_collection]
            local destination_args_names = {}
            local destination_args_values = {}

            for _, part in ipairs(v.parts) do
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
            local extra = {
                qcontext = info.qcontext
            }
            local object_args_instance = {} -- passed to 'filter'
            local list_args_instance = {} -- passed to 'args'

            local c_args = collection_to_arguments[destination_collection]
            local c_list_args = collection_to_list_arguments[destination_collection]

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
                local objs = state.accessor:select(parent,
                    v.destination_collection, from,
                    object_args_instance, list_args_instance, extra)
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
        end
        }
    return field
    end

--- The function converts passed connection to a field of GraphQL type
---
--- @tparam table state for read state.accessor and previously filled
--- state.types (state.types are gql types)
--- @tparam table connection connection to create field on
--- @tparam table collection_name name of the collection which have given
--- connection
local convert_connection_to_field = function(state, connection, collection_name)
    assert(type(connection.type) == 'string',
    'connection.type must be a string, got ' .. type(connection.type))
    assert(connection.type == '1:1' or connection.type == '1:N',
    'connection.type must be 1:1 or 1:N, got ' .. connection.type)
    assert(type(connection.name) == 'string',
    'connection.name must be a string, got ' .. type(connection.name))
    assert(connection.destination_collection or connection.variants,
        'connection must either destination_collection or variatns field')

    if connection.destination_collection then
        return convert_simple_connection(state, connection, collection_name)
    end

    if connection.variants then
        return convert_union_connection(state, connection, collection_name)
    end
end

--- The function converts passed avro-schema to a GraphQL type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types (state.types are gql types)
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
---
--- XXX As it is not clear now what to do with complex types inside arrays
--- (just pass to results or allow to use filters), only scalar arrays
--- is allowed for now. Note: map is considered scalar.
gql_type = function(state, avro_schema, collection, collection_name)
    assert(type(state) == 'table',
        'state must be a table, got ' .. type(state))
    assert(avro_schema ~= nil,
        'avro_schema must not be nil')
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

    -- type of the top element in the avro-schema
    local avro_t = avro_type(avro_schema)

    if avro_t == 'record' or avro_t == 'record*' then
        assert(type(avro_schema.name) == 'string',
            ('avro_schema.name must be a string, got %s (avro_schema %s)')
            :format(type(avro_schema.name), json.encode(avro_schema)))
        assert(type(avro_schema.fields) == 'table',
            ('avro_schema.fields must be a table, got %s (avro_schema %s)')
            :format(type(avro_schema.fields), json.encode(avro_schema)))

        local fields = convert_record_fields(state, avro_schema.fields)

        -- if collection param is passed then go over all connections
        for _, c in ipairs((collection or {}).connections or {}) do
            fields[c.name] = convert_connection_to_field(state, c, collection_name)
        end

        -- create gql type
        local res = types.object({
            name = collection ~= nil and collection.name or avro_schema.name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        })
        return avro_t == 'enum' and types.nonNull(res) or res
    elseif avro_t == 'enum' then
        error('enums not implemented yet') -- XXX
    elseif avro_t == 'array' or avro_t == 'array*' then
        assert(avro_schema.items ~= nil,
            'items field must not be nil in array avro schema')
        assert(type(avro_schema.items) == 'string'
            or type(avro_schema.items) == 'table',
            'avro_schema.items must be a string or a table, got ' ..
            type(avro_schema.items))

        local gql_items_type = gql_type(state, avro_schema.items)
        local gql_array = types.list(gql_items_type)
        return avro_t == 'array' and types.nonNull(gql_array) or gql_array
    elseif avro_t == 'map' or avro_t == 'map*' then
        assert(avro_schema.values ~= nil,
            'values must not be nil in map avro schema')
        assert(type(avro_schema.values) == 'table'
            or type(avro_schema.values) == 'string',
            ('avro_schema.values must be a table or a string, ' ..
            'got %s (avro_schema %s)'):format(type(avro_schema.values),
            json.encode(avro_schema)))

        -- validate avro schema format inside 'values'
        gql_type(state, avro_schema.values)

        local gql_map = types_map
        return avro_t == 'map' and types.nonNull(gql_map) or gql_map
    else
        local res = convert_scalar_type(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' ..
                json.encode(avro_schema))
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

    for collection_name, collection in pairs(state.collections) do
        collection.name = collection_name
        assert(collection.schema_name ~= nil,
            'collection.schema_name must not be nil')

        local schema = cfg.schemas[collection.schema_name]
        assert(schema ~= nil, ('cfg.schemas[%s] must not be nil'):format(
            tostring(collection.schema_name)))
        assert(schema.name == nil or schema.name == collection.schema_name,
            ('top-level schema name does not match the name in ' ..
            'the schema itself: "%s" vs "%s"'):format(collection.schema_name,
            schema.name))

        -- recursively converts all avro types into GraphQL types in the given
        -- schema
        assert(schema.type == 'record',
            'top-level schema must have record avro type, got ' ..
            tostring(schema.type))
        state.types[collection_name] = gql_type(state, schema, collection,
            collection_name)

        -- prepare arguments' types
        local object_args = convert_record_fields_to_args(schema.fields,
            {skip_compound = true})
        local list_args = convert_record_fields_to_args(
            accessor:list_args(collection_name))

        local args = utils.merge_tables(object_args, list_args)

        state.object_arguments[collection_name] = object_args
        state.list_arguments[collection_name] = list_args
        state.all_arguments[collection_name] = args

        -- create entry points from collection names
        fields[collection_name] = {
            kind = types.nonNull(types.list(state.types[collection_name])),
            arguments = state.all_arguments[collection_name],
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

                local extra = {
                    qcontext = info.qcontext
                }


                return accessor:select(rootValue, collection_name, from,
                        object_args_instance, list_args_instance, extra)
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

--- The function checks that one and only one GraphQL operation
--- (query/mutation/subscription) is defined in the AST and it's type
--- is 'query' as mutations and subscriptions are not supported yet.
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

--- The function just makes some reasonable assertions on input
--- and then call graphql-lua execute.
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

--- The function parses a query string, validate the resulting query
--- against the GraphQL schema and provides an object with the function to
--- execute the query with specific variables values.
---
--- @tparam table state current state of graphql, including
--- schemas, collections and accessor
--- @tparam string query query string
local function gql_compile(state, query)
    assert(type(state) == 'table' and type(query) == 'string',
        'use :validate(...) instead of .validate(...)')
    assert(state.schema ~= nil, 'have not compiled schema')

    local ast = parse(query)
    assert_gql_query_ast('gql_compile', ast)
    local operation_name = ast.definitions[1].name.value
    --
    --print('print from gql_compile - state.schema')
    --require('pl.pretty').dump(state.schema)
    --
    --print('ast')
    --require('pl.pretty').dump(ast)


    --@todo add custom validation for schemas with unions
    --or change insides of validate to process unions the custom way
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
---                     object_args_instance, list_args_instance, extra)
---                 -- from is nil for a top-level object, otherwise it is
---                 --
---                 -- {
---                 --     collection_name = <...>,
---                 --     connection_name = <...>,
---                 --     destination_args_names = <...>,
---                 --     destination_args_values = <...>,
---                 -- }
---                 --
---                 -- extra is a table which contains additional data for the
---                 -- query; by now it consists of a single qcontext table,
---                 -- which can be used by accessor to store any query-related
---                 -- data
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

--- Abstraction layer between a data collections (e.g. tarantool's spaces) and
--- the GraphQL query language.
---
--- Random notes:
---
--- * GraphQL top level statement must be a collection name. Arguments for this
---   statement match non-deducible field names of corresponding object and
---   passed to an accessor function in the filter argument.
---
--- Border cases:
---
--- * Unions: as GraphQL specification says "...no fields may be queried on
---   Union type without the use of typed fragments." Tarantool_graphql
---   behaves this way. So 'common fields' are not supported. This does NOT
---   work:
---
--- ```
--- hero {
---     hero_id -- common field; does NOT work
---     ... on human {
---         name
---     }
---     ... on droid {
---         model
---     }
--- }
--- ```
---
--- (GraphQL spec: http://facebook.github.io/graphql/October2016/#sec-Unions)
--- Also, no arguments are currently allowed for fragments.
--- See issue about this (https://github.com/facebook/graphql/issues/204)

local json = require('json')
local yaml = require('yaml')

local parse = require('graphql.core.parse')
local schema = require('graphql.core.schema')
local types = require('graphql.core.types')
local validate = require('graphql.core.validate')
local execute = require('graphql.core.execute')
local query_to_avro = require('graphql.query_to_avro')

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

local function nullable(gql_class)
    assert(type(gql_class) == 'table', 'gql_class must be a table, got ' ..
        type(gql_class))

    if gql_class.__type ~= 'NonNull' then return gql_class end

    assert(gql_class.ofType ~= nil, 'gql_class.ofType must not be nil')
    return nullable(gql_class.ofType)
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
--- InputObject instead of Object.
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
--- state.nullable_collection_types
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

local function args_from_destination_collection(state, collection,
                                                connection_type)
    if connection_type == '1:1' then
        return state.object_arguments[collection]
    elseif connection_type == '1:1*' then
        return state.object_arguments[collection]
    elseif connection_type == '1:N' then
        return state.all_arguments[collection]
    else
        error('unknown connection type: ' .. tostring(connection_type))
    end
end

local function specify_destination_type(destination_type, connection_type)
    if connection_type == '1:1' then
        return types.nonNull(destination_type)
    elseif connection_type == '1:1*' then
        return destination_type
    elseif connection_type == '1:N' then
        return types.nonNull(types.list(types.nonNull(destination_type)))
    else
        error('unknown connection type: ' .. tostring(connection_type))
    end
end

local function parent_args_values(parent, connection_parts)
    local destination_args_names = {}
    local destination_args_values = {}
    for _, part in ipairs(connection_parts) do
        assert(type(part.source_field) == 'string',
            'part.source_field must be a string, got ' ..
            type(part.destination_field))
        assert(type(part.destination_field) == 'string',
            'part.destination_field must be a string, got ' ..
            type(part.destination_field))

        destination_args_names[#destination_args_names + 1] =
            part.destination_field
        local value = parent[part.source_field]
        destination_args_values[#destination_args_values + 1] = value
    end

    return destination_args_names, destination_args_values
end

-- Check FULL match constraint before request of
-- destination object(s). Note that connection key parts
-- can be prefix of index key parts. Zero parts count
-- considered as ok by this check.
local function are_all_parts_null(parent, connection_parts)
    local are_all_parts_null = true
    local are_all_parts_non_null = true
    for _, part in ipairs(connection_parts) do
        local value = parent[part.source_field]

        if value ~= nil then -- nil or box.NULL
            are_all_parts_null = false
        else
            are_all_parts_non_null = false
        end
    end

    local ok = are_all_parts_null or are_all_parts_non_null
    if not ok then -- avoid extra json.encode()
        assert(ok,
            'FULL MATCH constraint was failed: connection ' ..
            'key parts must be all non-nulls or all nulls; ' ..
            'object: ' .. json.encode(parent))
    end

    return are_all_parts_null
end

local function separate_args_instance(args_instance, connection_args,
                                      connection_list_args)
    local object_args_instance = {}
    local list_args_instance = {}
    for k, v in pairs(args_instance) do
        if connection_list_args[k] ~= nil then
            list_args_instance[k] = v
        elseif connection_args[k] ~= nil then
            object_args_instance[k] = v
        else
            error(('cannot found "%s" field ("%s" value) ' ..
                'within allowed fields'):format(tostring(k),
                    json.encode(v)))
        end
    end
    return object_args_instance, list_args_instance
end

--- The function converts passed simple connection to a field of GraphQL type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.nullable_collection_types (those are gql types)
--- @tparam table avro_schema input avro-schema
--- @tparam[opt] table collection table with schema_name, connections fields
--- described a collection (e.g. tarantool's spaces)
---
--- @tparam table state for for collection types
--- @tparam table connection simple connection to create field on
--- @tparam table collection_name name of the collection which has given
--- connection
local function convert_simple_connection(state, connection, collection_name)
    local c = connection
    assert(type(c.destination_collection) == 'string',
        'connection.destination_collection must be a string, got ' ..
        type(c.destination_collection))
    assert(type(c.parts) == 'table',
        'connection.parts must be a table, got ' .. type(c.parts))

    -- gql type of connection field
    local destination_type =
        state.nullable_collection_types[c.destination_collection]
    assert(destination_type ~= nil,
        ('destination_type (named %s) must not be nil'):format(
        c.destination_collection))
    local raw_destination_type = destination_type

    local c_args = args_from_destination_collection(state,
        c.destination_collection, c.type)
    destination_type = specify_destination_type(destination_type, c.type)

    local c_list_args = state.list_arguments[c.destination_collection]

    -- capture `raw_destination_type`
    local function genResolveField(info)
        return function(field_name, object, filter, opts)
            assert(raw_destination_type.fields[field_name],
                ('performing a subrequest by the non-existent ' ..
                'field "%s" of the collection "%s"'):format(field_name,
                c.destination_collection))
            return raw_destination_type.fields[field_name].resolve(
                object, filter, info, opts)
        end
    end

    local field = {
        name = c.name,
        kind = destination_type,
        arguments = c_args,
        -- captures c.{parts, name, destination_collection}, collection_name,
        -- genResolveField, c_args, c_list_args.
        resolve = function(parent, args_instance, info, opts)
            local opts = opts or {}
            assert(type(opts) == 'table',
                'opts must be nil or a table, got ' .. type(opts))
            local dont_force_nullability =
                opts.dont_force_nullability or false
            assert(type(dont_force_nullability) == 'boolean',
                'opts.dont_force_nullability ' ..
                'must be nil or a boolean, got ' ..
                type(dont_force_nullability))

            local destination_args_names, destination_args_values =
                parent_args_values(parent, c.parts)

            -- Avoid non-needed index lookup on a destination
            -- collection when all connection parts are null:
            -- * return null for 1:1* connection;
            -- * return {} for 1:N connection (except the case when
            --   source collection is the Query pseudo-collection).
            if collection_name ~= 'Query' and are_all_parts_null(parent, c.parts)
                then
                    if c.type ~= '1:1*' and c.type ~= '1:N' then
                        -- `if` is to avoid extra json.encode
                        assert(c.type == '1:1*' or c.type == '1:N',
                            ('only 1:1* or 1:N connections can have ' ..
                            'all key parts null; parent is %s from ' ..
                            'collection "%s"'):format(json.encode(parent),
                                tostring(collection_name)))
                    end
                    return c.type == '1:N' and {} or nil
            end

            local from = {
                collection_name = collection_name,
                connection_name = c.name,
                destination_args_names = destination_args_names,
                destination_args_values = destination_args_values,
            }
            local resolveField = genResolveField(info)
            local extra = {
                qcontext = info.qcontext,
                resolveField = resolveField, -- for subrequests
            }

            -- object_args_instance will be passed to 'filter'
            -- list_args_instance will be passed to 'args'
            local object_args_instance, list_args_instance =
                separate_args_instance(args_instance, c_args, c_list_args)

            local objs = state.accessor:select(parent,
                c.destination_collection, from,
                object_args_instance, list_args_instance, extra)
            assert(type(objs) == 'table',
                'objs list received from an accessor ' ..
                'must be a table, got ' .. type(objs))
            if c.type == '1:1' or c.type == '1:1*' then
                -- we expect here exactly one object even for 1:1*
                -- connections because we processed all-parts-are-null
                -- situation above
                assert(#objs == 1 or dont_force_nullability,
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

--- The function converts passed union connection to a field of GraphQL type.
--- It combines destination collections of passed union connection into
--- the Union GraphQL type.
--- (destination collections are 'types' of a 'Union' in GraphQL).
---
--- @tparam table state for collection types
--- @tparam table connection union connection to create field on
--- @tparam table collection_name name of the collection which has given
--- connection
local function convert_union_connection(state, connection, collection_name)
    local c = connection
    local union_types = {}
    local collection_to_arguments = {}
    local collection_to_list_arguments = {}

    for _, v in ipairs(c.variants) do
        assert(v.determinant, 'each variant should have a determinant')
            assert(type(v.determinant) == 'table', 'variant\'s determinant ' ..
            'must end be a table, got ' .. type(v.determinant))
        assert(type(v.destination_collection) == 'string',
            'variant.destination_collection must be a string, got ' ..
            type(v.destination_collection))
        assert(type(v.parts) == 'table',
            'variant.parts must be a table, got ' .. type(v.parts))

        local destination_type =
            state.nullable_collection_types[v.destination_collection]
        assert(destination_type ~= nil,
            ('destination_type (named %s) must not be nil'):format(
                v.destination_collection))

        local v_args = args_from_destination_collection(state,
            v.destination_collection, c.type)
        destination_type = specify_destination_type(destination_type, c.type)

        local v_list_args = state.list_arguments[v.destination_collection]

        union_types[#union_types + 1] = destination_type

        collection_to_arguments[v.destination_collection] = v_args
        collection_to_list_arguments[v.destination_collection] = v_list_args
    end

    local determinant_keys = utils.get_keys(c.variants[1].determinant)

    local resolve_variant = function (parent)
        assert(utils.do_have_keys(parent, determinant_keys),
            ('Parent object of union object doesn\'t have determinant ' ..
            'fields which are necessary to determine which resolving ' ..
            'variant should be used. Union parent object:\n"%s"\n' ..
            'Determinant keys:\n"%s"'):
            format(yaml.encode(parent), yaml.encode(determinant_keys)))

        local variant_num
        local resulting_variant
        for i, variant in ipairs(c.variants) do
            variant_num = i
            local is_match = utils.is_subtable(parent, variant.determinant)

            if is_match then
                resulting_variant = variant
                break
            end
        end

        assert(resulting_variant, ('Variant resolving failed.'..
            'Parent object: "%s"\n'):format(yaml.encode(parent)))
        return resulting_variant, variant_num
    end

    local field = {
        name = c.name,
        kind = types.union({
            name = c.name,
            types = union_types,
        }),
        arguments = nil, -- see Border cases/Unions at the top of the file
        resolve = function(parent, args_instance, info)
            local v, variant_num = resolve_variant(parent)
            local destination_type = union_types[variant_num]
            local destination_collection =
                state.nullable_collection_types[v.destination_collection]
            local destination_args_names, destination_args_values =
                parent_args_values(parent, v.parts)

            -- Avoid non-needed index lookup on a destination
            -- collection when all connection parts are null:
            -- * return null for 1:1* connection;
            -- * return {} for 1:N connection (except the case when
            --   source collection is the Query pseudo-collection).
            if collection_name ~= 'Query' and are_all_parts_null(parent, v.parts)
                then
                    if c.type ~= '1:1*' and c.type ~= '1:N' then
                        -- `if` is to avoid extra json.encode
                        assert(c.type == '1:1*' or c.type == '1:N',
                            ('only 1:1* or 1:N connections can have ' ..
                            'all key parts null; parent is %s from ' ..
                            'collection "%s"'):format(json.encode(parent),
                                tostring(collection_name)))
                    end
                    return c.type == '1:N' and {} or nil, destination_type
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

            local c_args = collection_to_arguments[destination_collection]
            local c_list_args = collection_to_list_arguments[destination_collection]

            --object_args_instance -- passed to 'filter'
            --list_args_instance -- passed to 'args'

            local object_args_instance, list_args_instance =
                separate_args_instance(args_instance, c_args, c_list_args)

            local objs = state.accessor:select(parent,
                v.destination_collection, from,
                object_args_instance, list_args_instance, extra)
            assert(type(objs) == 'table',
                'objs list received from an accessor ' ..
                'must be a table, got ' .. type(objs))
            if c.type == '1:1' or c.type == '1:1*' then
                -- we expect here exactly one object even for 1:1*
                -- connections because we processed all-parts-are-null
                -- situation above
                assert(#objs == 1, 'expect one matching object, got ' ..
                    tostring(#objs))
                return objs[1], destination_type
            else -- c.type == '1:N'
                return objs, destination_type
            end
        end
    }
    return field
end

--- The function converts passed connection to a field of GraphQL type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types (state.types are gql types)
--- @tparam table connection connection to create field on
--- @tparam table collection_name name of the collection which have given
--- connection
--- @treturn table simple and union connection depending on the type of
--- input connection
local convert_connection_to_field = function(state, connection, collection_name)
    assert(type(connection.type) == 'string',
        'connection.type must be a string, got ' .. type(connection.type))
    assert(connection.type == '1:1' or connection.type == '1:1*' or
        connection.type == '1:N', 'connection.type must be 1:1, 1:1* or 1:N, '..
        'got ' .. connection.type)
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
--- state.nullable_collection_types (those are gql types)
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
        return avro_t == 'record' and types.nonNull(res) or res
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

--- Create virtual root collection `Query`, which has connections to any
--- collection.
---
--- Actually, each GQL query starts its execution from the `Query` collection.
--- That is why it shoult contain connections to any collection.
---
--- @tparam table state dictionary which contains all information about the
--- schema, arguments, types...
local function create_root_collection(state)
    local root_connections = {}
    -- The fake connections have 1:N mechanics.
    -- Create one connection for each collection.
    for collection_name, collection in pairs(state.collections) do
        table.insert(root_connections, {
            parts = {},
            name = collection_name,
            destination_collection = collection_name,
            type = "1:N"
        })
    end
    local root_schema = {
        type = "record",
        name = "Query",
        -- The fake root has no fields.
        fields = {}
    }
    local root_collection = {
        name = "Query",
        connections = root_connections
    }

    -- `gql_type` is designed to create GQL type corresponding to a real schema
    -- and connections. However it also works with the fake schema.
    -- Query type must be the Object, so it cannot be nonNull.
    local root_type = gql_type(state, root_schema, root_collection, "Query")
    state.schema = schema.create({
        query = nullable(root_type),
    })
end

--- Execute a function for each 1:1 or 1:1* connection of each collection.
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam function func a function with the following parameters:
---
--- * source collection name (string);
--- * connection (table).
local function for_each_1_1_connection(state, func)
    for collection_name, collection in pairs(state.collections) do
        for _, c in ipairs(collection.connections or {}) do
            if c.type == '1:1' or c.type == '1:1*' then
                func(collection_name, c)
            end
        end
    end
end

--- Add arguments corresponding to 1:1 and 1:1* connections (nested filters).
---
--- @tparam table state graphql_tarantool instance
local function add_connection_arguments(state)
    -- map destination collection to list of input objects
    local input_objects = {}
    -- map source collection and connection name to an input object
    local lookup_input_objects = {}

    -- create InputObjects for each 1:1 or 1:1* connection of each collection
    for_each_1_1_connection(state, function(collection_name, c)
        -- XXX: support union collections
        if c.variants ~= nil then return end

        local object = types.inputObject({
            name = c.name,
            description = ('generated from the connection "%s" ' ..
                'of collection "%s" using collection "%s"'):format(
                c.name, collection_name, c.destination_collection),
            fields = state.object_arguments[c.destination_collection],
        })

        if input_objects[c.destination_collection] == nil then
            input_objects[c.destination_collection] = {}
        end
        table.insert(input_objects[c.destination_collection], object)

        if lookup_input_objects[collection_name] == nil then
            lookup_input_objects[collection_name] = {}
        end
        lookup_input_objects[collection_name][c.name] = object
    end)

    -- update fields of collection arguments and input objects with other input
    -- objects
    for_each_1_1_connection(state, function(collection_name, c)
        -- XXX: support union collections
        if c.variants ~= nil then return end

        local new_object = lookup_input_objects[collection_name][c.name]
        -- collection arguments
        local fields = state.object_arguments[collection_name]
        assert(fields[c.name] == nil,
            'we must not add an input object twice to the same collection ' ..
            'arguments list')
        fields[c.name] = new_object
        -- input objects
        for _, input_object in ipairs(input_objects[collection_name] or {}) do
            local fields = input_object.fields
            assert(fields[c.name] == nil,
                'we must not add an input object twice to the same input ' ..
                'object')
            fields[c.name] = {
                name = c.name,
                kind = new_object,
            }
        end
    end)
end

local function parse_cfg(cfg)
    local state = {}

    -- collection type is always record, so always non-null; we can lazily
    -- evaluate non-null type from nullable type, but not vice versa, so we
    -- collect nullable types here and evaluate non-null ones where needed
    state.nullable_collection_types = utils.gen_booking_table({})

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

    -- Prepare types which represents:
    --  - Avro schemas (collections)
    --  - scalar field arguments (used to filter objects by value stored in it's
    --    field)
    --  - list arguments (offset, limit...)
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
        local collection_type =
            gql_type(state, schema, collection, collection_name)
        -- we utilize the fact that collection type is always non-null and
        -- don't store this information; see comment above for
        -- `nullable_collection_types` variable definition
        assert(collection_type.__type == 'NonNull',
            'collection must always has non-null type')
        state.nullable_collection_types[collection_name] =
            nullable(collection_type)

        -- prepare arguments' types
        local object_args = convert_record_fields_to_args(schema.fields,
            {skip_compound = true})
        local list_args = convert_record_fields_to_args(
            accessor:list_args(collection_name))

        state.object_arguments[collection_name] = object_args
        state.list_arguments[collection_name] = list_args
    end

    add_connection_arguments(state)

    -- fill all_arguments with object_arguments + list_arguments
    for collection_name, collection in pairs(state.collections) do
        local object_args = state.object_arguments[collection_name]
        local list_args = state.list_arguments[collection_name]

        local args = utils.merge_tables(object_args, list_args)
        state.all_arguments[collection_name] = args
    end

    -- create fake root `Query` collection
    create_root_collection(state)

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

    validate(state.schema, ast)

    local qstate = {
        state = state,
        ast = ast,
        operation_name = operation_name,
    }

    local gql_query = setmetatable(qstate, {
        __index = {
            execute = gql_execute,
            avro_schema = query_to_avro.convert
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
---                 -- `extra` is a table which contains additional data for
---                 -- the query:
---                 --
---                 -- * `qcontext` (table) can be used by an accessor to store
---                 --   any query-related data;
---                 -- * `resolveField(field_name, object, filter, opts)`
---                 -- (function) for performing a subrequest on a fields
---                 -- connected using a 1:1 or 1:1* connection.
---                 --
---                 return ...
---             end,
---             list_args = function(self, collection_name)
---                 return {
---                     {name = 'limit', type = 'int'},
---                     {name = 'offset', type = <...>}, -- type of a primary key
---                     {name = 'pcre', type = <...>},
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

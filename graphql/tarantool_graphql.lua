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
---     hero {
---         hero_id -- common field; does NOT work
---         ... on human {
---             name
---         }
---         ... on droid {
---             model
---         }
---     }
---
---
---
--- (GraphQL spec: http://facebook.github.io/graphql/October2016/#sec-Unions)
--- Also, no arguments are currently allowed for fragments.
--- See issue about this (https://github.com/facebook/graphql/issues/204)

local json = require('json')
local yaml = require('yaml')
local log = require('log')

local accessor_space = require('graphql.accessor_space')
local accessor_shard = require('graphql.accessor_shard')
local parse = require('graphql.core.parse')
local schema = require('graphql.core.schema')
local types = require('graphql.core.types')
local validate = require('graphql.core.validate')
local execute = require('graphql.core.execute')
local query_to_avro = require('graphql.query_to_avro')
local simple_config = require('graphql.simple_config')
local config_complement = require('graphql.config_complement')
local server = require('graphql.server.server')

local utils = require('graphql.utils')
local check = utils.check

local tarantool_graphql = {}
-- instance of tarantool graphql to provide graphql:compile() and
-- graphql:execute() method (with creating zero configuration graphql instance
-- under hood when calling compile() for the first time)
local default_instance

-- forward declarations
local gql_type

local function is_scalar_type(avro_schema_type)
    check(avro_schema_type, 'avro_schema_type', 'string')

    local scalar_types = {
        ['int'] = true,
        ['int*'] = true,
        ['long'] = true,
        ['long*'] = true,
        ['float'] = true,
        ['float*'] = true,
        ['double'] = true,
        ['double*'] = true,
        ['boolean'] = true,
        ['boolean*'] = true,
        ['string'] = true,
        ['string*'] = true,
        ['null'] = true,
    }

    return scalar_types[avro_schema_type] or false
end

local function is_comparable_scalar_type(avro_schema_type)
    check(avro_schema_type, 'avro_schema_type', 'string')

    local scalar_types = {
        ['int'] = true,
        ['int*'] = true,
        ['long'] = true,
        ['long*'] = true,
        ['boolean'] = true,
        ['boolean*'] = true,
        ['string'] = true,
        ['string*'] = true,
        ['null'] = true,
    }

    return scalar_types[avro_schema_type] or false
end

local function is_compound_type(avro_schema_type)
    check(avro_schema_type, 'avro_schema_type', 'string')

    local compound_types = {
        ['record'] = true,
        ['record*'] = true,
        ['array'] = true,
        ['array*'] = true,
        ['map'] = true,
        ['map*'] = true,
    }

    return compound_types[avro_schema_type] or false
end

local function avro_type(avro_schema, opts)
    local opts = opts or {}
    local allow_references = opts.allow_references or false

    if type(avro_schema) == 'table' then
        if utils.is_array(avro_schema) then
            return 'union'
        elseif is_compound_type(avro_schema.type) then
            return avro_schema.type
        elseif allow_references then
            return avro_schema
        end
    elseif type(avro_schema) == 'string' then
        if is_scalar_type(avro_schema) then
            return avro_schema
        elseif allow_references then
            return avro_schema
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

local function raw_gql_type(gql_class)
    assert(type(gql_class) == 'table', 'gql_class must be a table, got ' ..
        type(gql_class))

    while gql_class.ofType ~= nil do
        gql_class = gql_class.ofType
    end

    return gql_class
end

local types_long = types.scalar({
    name = 'Long',
    description = 'Long is non-bounded integral type',
    serialize = function(value) return tonumber(value) end,
    parseValue = function(value) return tonumber(value) end,
    parseLiteral = function(node)
        -- 'int' is name of the immediate value type
        if node.kind == 'int' then
            return tonumber(node.value)
        end
    end
})

local types_double = types.scalar({
    name = 'Double',
    serialize = tonumber,
    parseValue = tonumber,
    parseLiteral = function(node)
        -- 'float' and 'int' are names of immediate value types
        if node.kind == 'float' or node.kind == 'int' then
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

local function convert_scalar_type(avro_schema, opts)
    local opts = opts or {}
    assert(type(opts) == 'table', 'opts must be nil or table, got ' ..
        type(opts))
    local raise = opts.raise or false
    assert(type(raise) == 'boolean', 'opts.raise must be boolean, got ' ..
        type(raise))

    local scalar_types = {
        ['int'] = types.int.nonNull,
        ['int*'] = types.int,
        ['long'] = types_long.nonNull,
        ['long*'] = types_long,
        ['float'] = types.float.nonNull,
        ['float*'] = types.float,
        ['double'] = types_double.nonNull,
        ['double*'] = types_double,
        ['boolean'] = types.boolean.nonNull,
        ['boolean*'] = types.boolean,
        ['string'] = types.string.nonNull,
        ['string*'] = types.string,
    }

    local avro_t = avro_type(avro_schema)
    local graphql_type = scalar_types[avro_t]
    if graphql_type ~= nil then
        return graphql_type
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
                kind = gql_field_type,
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
--- * `dont_skip` -- do not skip any fields; default: false.
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

    local dont_skip = opts.dont_skip or false
    check(dont_skip, 'dont_skip', 'boolean')

    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))
        -- records, arrays (gql lists), maps and unions can't be arguments, so
        -- these graphql types are to be skipped;
        -- skip_compound == false is the trick for accessor_general-provided
        -- record; we don't expect map, array or union here as well as we don't
        -- expect avro-schema reference.
        local avro_t = avro_type(field.type, {allow_references = true})
        local add_field = dont_skip or is_comparable_scalar_type(avro_t) or
            (not skip_compound and not is_scalar_type(avro_t))
        if add_field then
            local ok, gql_class = pcall(gql_argument_type, field.type)
            -- XXX: we need better avro-schema -> graphql types converter to
            -- handle the following cases:
            -- * scalar arguments that can be checked for equality (object
            --   args): skip any other
            -- * pcre / limit / offset (nothing special here I guess)
            -- * auxiliary schemas for insert / update: don't skip anything
            if ok then
                args[field.name] = nullable(gql_class)
            else
                log.warn(('Cannot add argument "%s": %s'):format(
                    field.name, tostring(gql_class)))
            end
        end
    end
    return args
end

--- Convert each field of an avro-schema to a graphql type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.nullable_collection_types
---
--- @tparam table fields fields part from an avro-schema
---
--- @tparam table context as described in @{gql_type}; not used here
--- explicitly, but `path` and `field_name` are *updated* and the `context` is
--- passed deeper within the @{gql_type} call
---
--- @treturn table `res` -- map with type names as keys and graphql types as
--- values
local function convert_record_fields(state, fields, context)
    local res = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))

        table.insert(context.path, field.name)
        context.field_name = field.name
        res[field.name] = {
            name = field.name,
            kind = gql_type(state, field.type, context),
        }
        table.remove(context.path, #context.path)
        context.field_name = nil
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

--- The function 'boxes' given collection type.
---
--- Why the 'boxing' of collection types is needed and how it is done is
--- described in comments to @{convert_multihead_connection}.
---
--- @tparam table type_to_box GraphQL Object type (which represents a collection)
--- @tparam string connection_type of given collection (1:1, 1:1* or 1:N)
--- @tparam string type_to_box_name name of given 'type_to_box' (It can not
--- be taken from 'type_to_box' because at the time of function execution
--- 'type_to_box' refers to an empty table, which later will be filled with
--- actual type table)
--- @treturn table GraphQL Object type representing 'boxed' collection
--- @treturn string name of the single field in the box GraphQL Object
local function box_collection_type(type_to_box, connection_type, type_to_box_name)
    check(type_to_box, 'type_to_box', 'table')
    check(connection_type, 'connection_type', 'string')
    check(type_to_box_name, 'type_to_box_name', 'string')

    local box_type_name
    local box_type_description

    if connection_type == '1:1' then
        box_type_name = 'box_' .. type_to_box_name
        box_type_description = 'Box around 1:1 multi-head variant'
    elseif connection_type == '1:1*' then
        box_type_name = 'box_' .. type_to_box_name
        box_type_description = 'Box around 1:1* multi-head variant'
    elseif connection_type == '1:N' then
        box_type_name = 'box_array_' .. type_to_box_name
        box_type_description = 'Box around 1:N multi-head variant'
    else
        error('unknown connection type: ' .. tostring(connection_type))
    end

    local field_name = type_to_box_name
    local box_field = {[field_name] = {name = field_name, kind = type_to_box}}
    local box_type = types.object({
        name = box_type_name,
        description = box_type_description,
        fields = box_field
    })

    return box_type, field_name
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
                                      connection_list_args, extra_args)
    local object_args_instance = {}
    local list_args_instance = {}
    local extra_args_instance = {}

    for k, v in pairs(args_instance) do
        if extra_args[k] ~= nil then
            extra_args_instance[k] = v
        elseif connection_list_args[k] ~= nil then
            list_args_instance[k] = v
        elseif connection_args[k] ~= nil then
            object_args_instance[k] = v
        else
            error(('cannot found "%s" field ("%s" value) ' ..
                'within allowed fields'):format(tostring(k),
                    json.encode(v)))
        end
    end
    return object_args_instance, list_args_instance, extra_args_instance
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

    check(c.destination_collection, 'connection.destination_collection', 'string')
    check(c.parts, 'connection.parts', 'table')

    -- gql type of connection field
    local destination_type =
        state.nullable_collection_types[c.destination_collection]
    assert(destination_type ~= nil,
        ('destination_type (named %s) must not be nil'):format(
        c.destination_collection))

    local raw_destination_type = destination_type
    destination_type = specify_destination_type(destination_type, c.type)

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

    local c_args = args_from_destination_collection(state,
        c.destination_collection, c.type)
    local c_list_args = state.list_arguments[c.destination_collection]
    local e_args = state.extra_arguments[c.destination_collection]

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

            -- Avoid non-needed index lookup on a destination collection when
            -- all connection parts are null:
            -- * return null for 1:1* connection;
            -- * return {} for 1:N connection (except the case when source
            --   collection is the query or the mutation pseudo-collection).
            if collection_name ~= nil and are_all_parts_null(parent, c.parts)
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
                extra_args = {},
            }

            -- object_args_instance will be passed to 'filter'
            -- list_args_instance will be passed to 'args'
            -- extra_args_instance will be passed to 'extra.extra_args'
            local object_args_instance, list_args_instance,
                extra_args_instance = separate_args_instance(args_instance,
                    c_args, c_list_args, e_args)
            extra.extra_args = extra_args_instance

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

--- The function converts passed multi-head connection to GraphQL Union type.
---
--- Destination collections of passed multi-head connection are turned into
--- variants of resulting GraphQL Union type. Note that GraphQL types which
--- represent destination collections are wrapped with 'box' types. Here is 'how'
--- and 'why' it is done.
---
--- How:
--- Let's consider multi-head connection with two destination collections:
---     "human": {
---         "name": "human",
---         "type": "record",
---         "fields": [
---             { "name": "hero_id", "type": "string" },
---             { "name": "name", "type": "string" }
---         ]
---     }
---
---     "starship": {
---         "name": "starship",
---         "type": "record",
---         "fields": [
---             { "name": "hero_id", "type": "string" },
---             { "name": "model", "type": "string" }
---         ]
---     }
---
--- In case of 1:1 multi-head connection the resulting field can be accessed as
--- follows:
---     hero_connection {
---         ... on box_human_collection {
---             human_collection {
---                 name
---             }
---         }
---         ... on box_starship_collection {
---             starship_collection {
---                 model
---             }
---         }
---     }
---
--- In case of 1:N multi-head connection:
---     hero_connection {
---         ... on box_array_human_collection {
---             human_collection {
---                 name
---             }
---         }
---         ... on box_array_starship_collection {
---             starship_collection {
---                 model
---             }
---         }
---     }
---
--- Why:
--- There are two reasons for 'boxing'.
--- 1) In case of 1:N connections, destination collections are represented by
--- GraphQL Lists (of Objects). But according to the GraphQL specification only
--- Objects can be variants of Union. So we need to 'box' Lists (into Objects
--- with single field) to use them as Union variants.
--- 2) GraphQL responses, received from tarantool graphql, must be avro-valid.
--- On every incoming GraphQL query a corresponding avro-schema can be generated.
--- Response to this query is 'avro-valid' if it can be successfully validated with
--- this generated (from incoming query) avro-schema. In case of multi-head
--- connections it means that value of multi-head connection field must have
--- the following format: SomeDestinationCollectionType: {...} where {...}
--- indicates the YAML encoding of a SomeDestinationCollectionType instance.
--- In case of 1:N {...} indicates a list of instances. Using of 'boxing'
--- provides the needed format.
---
--- @tparam table state for collection types
--- @tparam table connection multi-head connection to create GraphQL Union on
--- @tparam table collection_name name of the collection which has given
--- connection
--- @treturn table GraphQL Union type
local function convert_multihead_connection(state, connection, collection_name)
    local c = connection
    local union_types = {}
    local collection_to_arguments = {}
    local collection_to_list_arguments = {}
    local collection_to_extra_arguments = {}
    local var_num_to_box_field_name = {}

    for _, v in ipairs(c.variants) do
        assert(v.determinant, 'each variant should have a determinant')
        check(v.determinant, 'variant\'s determinant', 'table')
        check(v.destination_collection, 'variant.destination_collection', 'string')
        check(v.parts, 'variant.parts', 'table')

        local destination_type =
            state.nullable_collection_types[v.destination_collection]
        assert(destination_type ~= nil,
            ('destination_type (named %s) must not be nil'):format(
                v.destination_collection))
        destination_type = specify_destination_type(destination_type, c.type)

        local variant_type, box_field_name = box_collection_type(destination_type,
            c.type, v.destination_collection)
        var_num_to_box_field_name[#union_types + 1] = box_field_name
        union_types[#union_types + 1] = variant_type

        local v_args = args_from_destination_collection(state,
            v.destination_collection, c.type)

        local v_list_args = state.list_arguments[v.destination_collection]
        local v_extra_args = state.extra_arguments[v.destination_collection]

        collection_to_arguments[v.destination_collection] = v_args
        collection_to_list_arguments[v.destination_collection] = v_list_args
        collection_to_extra_arguments[v.destination_collection] = v_extra_args
    end

    local determinant_keys = utils.get_keys(c.variants[1].determinant)

    local resolve_variant = function (parent)
        assert(utils.do_have_keys(parent, determinant_keys),
            ('Parent object of union object doesn\'t have determinant ' ..
            'fields which are necessary to determine which resolving ' ..
            'variant should be used. Union parent object:\n"%s"\n' ..
            'Determinant keys:\n"%s"'):
            format(yaml.encode(parent), yaml.encode(determinant_keys)))

        local var_idx
        local res_var
        for i, var in ipairs(c.variants) do
            local is_match = utils.is_subtable(parent, var.determinant)
            if is_match then
                res_var = var
                var_idx = i
                break
            end
        end

        local box_field_name = var_num_to_box_field_name[var_idx]

        assert(res_var, ('Variant resolving failed.'..
            'Parent object: "%s"\n'):format(yaml.encode(parent)))
        return res_var, var_idx, box_field_name
    end

    local field = {
        name = c.name,
        kind = types.union({
            name = c.name,
            types = union_types,
        }),
        arguments = nil, -- see Border cases/Unions at the top of the file
        resolve = function(parent, args_instance, info)
            local v, variant_num, box_field_name = resolve_variant(parent)
            local destination_type = union_types[variant_num]

            local destination_collection =
                state.nullable_collection_types[v.destination_collection]
            local destination_args_names, destination_args_values =
                parent_args_values(parent, v.parts)

            -- Avoid non-needed index lookup on a destination collection when
            -- all connection parts are null:
            -- * return null for 1:1* connection;
            -- * return {} for 1:N connection (except the case when source
            --   collection is the query or the mutation pseudo-collection).
            if collection_name ~= nil and are_all_parts_null(parent, v.parts)
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
                qcontext = info.qcontext,
                extra_args = {},
            }

            local c_args = collection_to_arguments[destination_collection]
            local c_list_args =
                collection_to_list_arguments[destination_collection]
            local e_args = collection_to_extra_arguments[destination_collection]

            -- object_args_instance will be passed to 'filter'
            -- list_args_instance will be passed to 'args'
            -- extra_args_instance will be passed to 'extra.extra_args'
            local object_args_instance, list_args_instance,
                extra_args_instance = separate_args_instance(args_instance,
                    c_args, c_list_args, e_args)
            extra.extra_args = extra_args_instance

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

                -- this 'wrapping' is needed because we use 'select' on
                -- 'collection' GraphQL type and the result of the resolve function
                -- must be in {'collection_name': {result}} format to
                -- be avro-valid
                local formatted_obj = {[box_field_name] = objs[1]}
                return formatted_obj, destination_type
            else -- c.type == '1:N'
                local formatted_objs = {[box_field_name] = objs}
                return formatted_objs, destination_type
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
        'connection must either destination_collection or variants field')

    if connection.destination_collection then
        return convert_simple_connection(state, connection, collection_name)
    end

    if connection.variants then
        return convert_multihead_connection(state, connection, collection_name)
    end
end

--- The function 'boxes' given GraphQL type into GraphQL Object 'box' type.
---
--- @tparam table type_to_box GraphQL type to be boxed
--- @tparam string box_field_name name of the single box field
--- @treturn table GraphQL Object
local function box_type(type_to_box, box_field_name)
    check(type_to_box, 'type_to_box', 'table')
    check(box_field_name, 'box_field_name', 'string')

    local gql_true_type = nullable(type_to_box)

    local box_name = gql_true_type.name or gql_true_type.__type
    box_name = box_name .. '_box'

    local box_fields = {[box_field_name] = {name = box_field_name,
        kind = type_to_box }}

    return types.object({
        name = box_name,
        description = 'Box (wrapper) around union variant',
        fields = box_fields,
    })
end

--- The functions creates table of GraphQL types from avro-schema union type.
---
--- @tparam table avro-schema
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table context as described in @{gql_type}; not used here
--- explicitly, but passed deeper within the @{gql_type} call
---
--- @treturn table union_types
---
--- @treturn table determinant_to_type
---
--- @treturn boolean is_nullable
local function create_union_types(avro_schema, state, context)
    check(avro_schema, 'avro_schema', 'table')
    assert(utils.is_array(avro_schema), 'union avro-schema must be an array ' ..
        ', got\n' .. yaml.encode(avro_schema))

    local union_types = {}
    local determinant_to_type = {}
    local is_nullable = false

    for _, type in ipairs(avro_schema) do
        -- If there is a 'null' type among 'union' types (in avro-schema union)
        -- then resulting GraphQL Union type will be nullable
        if type == 'null' then
            is_nullable = true
        else
            local variant_type = gql_type(state, type, context)
            local box_field_name = type.name or avro_type(type)
            union_types[#union_types + 1] = box_type(variant_type, box_field_name)
            local determinant = type.name or type.type or type
            determinant_to_type[determinant] = union_types[#union_types]
        end
    end

    return union_types, determinant_to_type, is_nullable
end

--- The function creates GraphQL Union type from given avro-schema union type.
--- There are two problems with GraphQL Union types, which we solve with specific
--- format of generated Unions. These problems are:
---
--- 1) GraphQL Unions represent an object that could be one of a list of
---    GraphQL Object types. So Scalars and Lists can not be one of Union
---    types.
---
--- 2) GraphQL responses, received from tarantool graphql, must be avro-valid.
---    On every incoming GraphQL query a corresponding avro-schema can be
---    generated. Response to this query is 'avro-valid' if it can be
---    successfully validated with this generated (from incoming query)
---    avro-schema.
---
--- Specific format of generated Unions include the following:
---
--- Avro scalar types (e.g. int, string) are converted into GraphQL Object types.
--- Avro scalar converted to GraphQL Scalar (string -> String) and then name of
--- GraphQL type is concatenated with '_box' ('String_box'). Resulting name is a name
--- of created GraphQL Object. This object has only one field with GraphQL type
--- corresponding to avro scalar type (String type in our example). Avro type's
--- name is taken as a name for this single field.
---
---     [..., "string", ...]
---
--- turned into
---     MyUnion {
---         ...
---         ... on String_box {
---             string
---         ...
---     }
---
--- Avro arrays and maps are converted into GraphQL Object types. The name of
--- the resulting GraphQL Object is 'List_box' or 'Map_box' respectively. This
--- object has only one field with GraphQL type corresponding to 'items'/'values'
--- avro type. 'array' or 'map' (respectively) is taken as a name of this
--- single field.
---
---     [..., {"type": "array", "items": "int"}, ...]
---
--- turned into
---     MyUnion {
---         ...
---         ... on List_box {
---             array
---         ...
---     }
---
--- Avro records are converted into GraphQL Object types. The name of the resulting
--- GraphQL Object is concatenation of record's name and '_box'. This Object
--- has only one field. The name of this field is record's name. The type of this
--- field is GraphQL Object generated from avro record schema in a usual way
--- (see @{gql_type})
---
---     { "type": "record", "name": "Foo", "fields":[
---         { "name": "foo1", "type": "string" },
---         { "name": "foo2", "type": "string" }
---     ]}
---
--- turned into
---     MyUnion {
---         ...
---         ... on Foo_box {
---             Foo {
---                 foo1
---                 foo2
---             }
---         ...
---     }
---
--- Please consider full example below.
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table avro_schema avro-schema union type
---
--- @tparam table context as described in @{gql_type}; only
--- `context.field_name` is used here (as the name of the generated GraphQL
--- union); `path` is *updated* (with the field name) and the `context` is
--- passed deeper within the @{create_union_types} call (which calls
--- @{gql_type} inside)
---
--- @treturn table GraphQL Union type. Consider the following example:
---
--- Avro-schema (inside a record):
---
---     ...
---     "name": "MyUnion", "type": [
---         "null",
---         "string",
---         { "type": "array", "items": "int" },
---         { "type": "record", "name": "Foo", "fields":[
---             { "name": "foo1", "type": "string" },
---             { "name": "foo2", "type": "string" }
---         ]}
---     ]
---     ...
---
--- GraphQL Union type (It will be nullable as avro-schema has 'null' variant):
---
---     MyUnion {
---         ... on String_box {
---             string
---         }
---
---         ... on List_box {
---             array
---         }
---
---         ... on Foo_box {
---             Foo {
---                 foo1
---                 foo2
---             }
---     }
local function create_gql_union(state, avro_schema, context)
    check(avro_schema, 'avro_schema', 'table')
    assert(utils.is_array(avro_schema), 'union avro-schema must be an ' ..
        'array, got:\n' .. yaml.encode(avro_schema))

    local union_name = context.field_name
    check(union_name, 'field_name', 'string')

    -- check avro-schema constraints
    for i, type in ipairs(avro_schema) do
        assert(avro_type(type) ~= 'union', 'unions must not immediately ' ..
            'contain other unions')

        if type.name ~= nil then
            for j, another_type in ipairs(avro_schema) do
                if i ~= j then
                    if another_type.name ~= nil then
                        assert(type.name:gsub('%*$', '') ~=
                            another_type.name:gsub('%*$', ''),
                            'Unions may not contain more than one schema ' ..
                                'with the same name')
                    end
                end
            end
        else
            for j, another_type in ipairs(avro_schema) do
                if i ~= j then
                    assert(avro_type(type) ~= avro_type(another_type),
                        'Unions may not contain more than one schema with ' ..
                            'the same type except for the named types: ' ..
                            'record, fixed and enum')
                end
            end
        end
    end

    -- create GraphQL union
    table.insert(context.path, union_name)
    local union_types, determinant_to_type, is_nullable =
        create_union_types(avro_schema, state, context)
    table.remove(context.path, #context.path)

    local union_type = types.union({
        types = union_types,
        name = union_name,
        resolveType = function(result)
            for determinant, type in pairs(determinant_to_type) do
                if result[determinant] ~= nil then
                    return type
                end
            end
            error(('result object has no determinant field matching ' ..
                'determinants for this union\nresult object:\n%s' ..
                'determinants:\n%s'):format(yaml.encode(result),
                    yaml.encode(determinant_to_type)))
        end
    })

    if not is_nullable then
        union_type = types.nonNull(union_type)
    end

    return union_type
end

--- The function converts passed avro-schema to a GraphQL type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.nullable_collection_types (those are gql types)
---
--- @tparam table avro_schema input avro-schema
---
--- @tparam table context current context of parsing the avro_schema, consists
--- the following fields:
---
--- * `collection` (table; optional) is a table with `schema_name` and
---   `connections` fields describes a collection (e.g. local tarantool spaces
---   or sharded spaces)
---
--- * `collection_name` (string; optional) name of the collection
---
--- * `definitions` (table) map from currently parsed avro-schema names to
---   generated GraphQL types; it allows reusing the same types w/o creation a
---   new same-named type, that considered as an error by graphql-lua when
---   creating type map for introspection
---
--- * `field_name` (string; optional) it is only for an union generation,
---   because avro-schema union has no name in it and specific name is
---   necessary for GraphQL union
---
--- * `path` (table) path to our position in avro-schema tree; it is used now
---   only to determine whether we are on the upmost level or on a nested one
---
--- If collection is passed connections from the collection will be taken into
--- account to automatically generate corresponding decucible fields.
---
--- If collection_name is passed it will be used as the resulting graphql type
--- name instead of the avro-schema name.
---
--- XXX As it is not clear now what to do with complex types inside arrays
--- (just pass to results or allow to use filters), only scalar arrays
--- is allowed for now. Note: map is considered scalar.
gql_type = function(state, avro_schema, context)
    check(state, 'state', 'table')
    assert(avro_schema ~= nil, 'avro_schema must not be nil')
    check(context, 'context', 'table')

    local collection = context.collection
    local collection_name = context.collection_name
    local definitions = context.definitions
    local field_name = context.field_name
    local path = context.path

    check(collection, 'collection', 'table', 'nil')
    check(collection_name, 'collection_name', 'string', 'nil')
    check(field_name, 'field_name', 'string', 'nil')
    check(definitions, 'definitions', 'table')
    check(path, 'path', 'table')

    local accessor = state.accessor
    assert(accessor ~= nil, 'state.accessor must not be nil')
    assert(accessor.select ~= nil, 'state.accessor.select must not be nil')
    assert(accessor.list_args ~= nil,
        'state.accessor.list_args must not be nil')

    -- type of the top element in the avro-schema
    local avro_t = avro_type(avro_schema, {allow_references = true})

    if avro_t == 'record' or avro_t == 'record*' then
        assert(type(avro_schema.name) == 'string',
            ('avro_schema.name must be a string, got %s (avro_schema %s)')
            :format(type(avro_schema.name), json.encode(avro_schema)))
        assert(type(avro_schema.fields) == 'table',
            ('avro_schema.fields must be a table, got %s (avro_schema %s)')
            :format(type(avro_schema.fields), json.encode(avro_schema)))

        local graphql_type_name = next(path) == nil and collection_name or
            avro_schema.name
        local def = definitions[graphql_type_name .. (avro_t:endswith('*')
            and '*' or '')]
        if def ~= nil then
            return def
        end

        local fields = convert_record_fields(state, avro_schema.fields, context)

        -- if collection param is passed then go over all connections
        for _, c in ipairs((collection or {}).connections or {}) do
            fields[c.name] = convert_connection_to_field(state, c, collection_name)
        end

        -- create gql type
        local res = types.object({
            name = graphql_type_name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        })
        assert(definitions[graphql_type_name] == nil and
            definitions[graphql_type_name .. '*'] == nil,
            'multiple definitions of ' .. graphql_type_name)
        definitions[graphql_type_name] = types.nonNull(res)
        definitions[graphql_type_name .. '*'] = res
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

        local gql_items_type = gql_type(state, avro_schema.items, context)
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
        gql_type(state, avro_schema.values, context)

        local gql_map = types_map
        return avro_t == 'map' and types.nonNull(gql_map) or gql_map
    elseif avro_t == 'union' then
        return create_gql_union(state, avro_schema, context)
    else
        if type(avro_schema) == 'string' then
            if definitions[avro_schema] ~= nil then
                return definitions[avro_schema]
            end
        end

        local res = convert_scalar_type(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' ..
                json.encode(avro_schema))
        end
        return res
    end
end

--- Add extra arguments for collection / connection fields.
---
--- XXX: This function is written in the hacky way. The function should gone
--- when we'll rewrite argument / InputObject generation in the right way. The
--- plan is the following:
---
--- * Move object_args to accessor_general (or move all *_args function into a
---   separate module); skipping float / double / ... arguments should be done
---   here.
--- * TBD: generate per-connection arguments in avro-schema in some way?
--- * Move avro-schema -> GraphQL arguments translating into its own module.
--- * Support a sub-record arguments and others (union, array, ...).
--- * Generate arguments for cartesian product of {1:1, 1:1*, 1:N, all} x
---   {query, mutation, all} x {top-level, nested, all} x {collections}.
--- * Use generated arguments in GraphQL types (schema) generation.
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table root_types generated by @{create_root_collection}
---
--- @return nothing
local function add_extra_arguments(state, root_types)
    for _, what in ipairs({'Query', 'Mutation'}) do
        -- add extra arguments to top-level fields (collections)
        for collection_name, field in pairs(root_types[what].fields) do
            -- Prevent exposing an argument inserted, say, into the mutation schema
            -- subtree to the query subtree (it is needed because we use a booking
            -- table for arguments).
            field.arguments = table.copy(field.arguments)

            local extra_args = state.extra_arguments[collection_name]
            local extra_args_meta = state.extra_arguments_meta[collection_name]

            for arg_name, arg in pairs(extra_args) do
                local meta = extra_args_meta[arg_name]
                check(meta, 'meta', 'table')
                local add_arg = what == 'Mutation' or
                    not meta.add_to_mutations_only
                if add_arg then
                    field.arguments[arg_name] = arg
                end
            end

            local parent_field = field

            local collection = state.collections[collection_name]
            for _, c in ipairs(collection.connections or {}) do
                -- XXX: support multihead connections
                if c.destination_collection then
                    local collection_name = c.destination_collection
                    local field = raw_gql_type(parent_field.kind).fields[c.name]
                    local extra_args = state.extra_arguments[collection_name]
                    local extra_args_meta =
                        state.extra_arguments_meta[collection_name]

                    for arg_name, arg in pairs(extra_args) do
                        local meta = extra_args_meta[arg_name]
                        check(meta, 'meta', 'table')
                        local add_arg = not meta.add_to_top_fields_only and
                            (what == 'Mutation' or
                            not meta.add_to_mutations_only)
                        if add_arg then
                            field.arguments[arg_name] = arg
                        end
                    end
                end
            end
        end
    end
end

--- Create virtual root collections `query` and `mutation`, which has
--- connections to any collection.
---
--- Actually, each GQL query starts its execution from the `query` or
--- `mutation` collection. That is why it shoult contain connections to any
--- collection.
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

    local root_types = {}

    for _, what in ipairs({'Query', 'Mutation'}) do
        local root_schema = {
            type = "record",
            name = what,
            -- The fake root has no fields.
            fields = {}
        }
        local root_collection = {
            name = what,
            connections = root_connections
        }
        local context = {
            collection = root_collection,
            collection_name = nil,
            definitions = {},
            field_name = nil,
            path = {},
        }

        -- `gql_type` is designed to create GQL type corresponding to a real
        -- schema and connections. However it also works with the fake schema.
        -- Query/Mutation type must be the Object, so it cannot be nonNull.
        root_types[what] = nullable(gql_type(state, root_schema, context))
    end

    add_extra_arguments(state, root_types)

    state.schema = schema.create({
        query = root_types['Query'],
        mutation = root_types['Mutation'],
    })
end

--- Execute a function for each 1:1 or 1:1* connection of each collection.
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table[opt] connection_types list of connection types to call `func`
--- on it; nil/box.NULL means all connections w/o filtering
---
--- @tparam function func a function with the following parameters:
---
--- * source collection name (string);
--- * connection (table).
local function for_each_connection(state, connection_types, func)
    for collection_name, collection in pairs(state.collections) do
        for _, c in ipairs(collection.connections or {}) do
            if connection_types == nil or utils.value_in(c.type,
                    connection_types) then
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
    for_each_connection(state, {'1:1', '1:1*'}, function(collection_name, c)
        -- XXX: support multihead connections
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
    for_each_connection(state, {'1:1', '1:1*'}, function(collection_name, c)
        -- XXX: support multihead connections
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

    -- Booking table used here because of the one reason: inside a resolve
    -- function we need to determine that a user-provided argument is an extra
    -- argument. We capture extra_arguments[collection_name] into the resolve
    -- function and sure it exists and will not be changed.
    state.extra_arguments = utils.gen_booking_table({})
    state.extra_arguments_meta = {}

    local context = {}

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

        assert(schema.type == 'record',
            'top-level schema must have record avro type, got ' ..
            tostring(schema.type))

        -- collection, collection_name are local for collection, definitions
        -- are local for top-level avro-schema
        if context[schema.name] == nil then
            context[schema.name] = {
                -- map from avro-schema names to graphql types
                definitions = {},
            }
        end
        context[schema.name].collection = collection
        context[schema.name].collection_name = collection_name
        context[schema.name].field_name = nil
        context[schema.name].path = {}

        -- recursively converts all avro types into GraphQL types in the given
        -- schema
        local collection_type =
            gql_type(state, schema, context[schema.name])
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
        local extra_args_avro, extra_args_meta = accessor:extra_args(
            collection_name)
        check(extra_args_meta, 'extra_args_meta', 'table')
        local extra_args = convert_record_fields_to_args(extra_args_avro,
            {dont_skip = true})

        state.object_arguments[collection_name] = object_args
        state.list_arguments[collection_name] = list_args
        state.extra_arguments[collection_name] = extra_args
        state.extra_arguments_meta[collection_name] = extra_args_meta
    end

    add_connection_arguments(state)

    -- fill all_arguments with object_arguments + list_arguments
    for collection_name, collection in pairs(state.collections) do
        local object_args = state.object_arguments[collection_name]
        local list_args = state.list_arguments[collection_name]

        local args = utils.merge_tables(object_args, list_args)
        state.all_arguments[collection_name] = args
    end

    -- create fake root for the `query` and the `mutation` collection
    create_root_collection(state)

    return state
end

--- Execute an operation from compiled query.
---
--- @tparam qstate compiled query
---
--- @tparam variables variables to pass to the query
---
--- @tparam[opt] string operation_name optional operation name
---
--- @treturn table result of the operation
local function gql_execute(qstate, variables, operation_name)
    assert(qstate.state)
    local state = qstate.state
    assert(state.schema)

    check(variables, 'variables', 'table')
    check(operation_name, 'operation_name', 'string', 'nil')

    local root_value = {}

    return execute(state.schema, qstate.ast, root_value, variables,
        operation_name)
end

--- Compile a query and execute an operation.
---
--- See @{gql_compile} and @{gql_execute} for parameters description.
---
--- @treturn table result of the operation
local function compile_and_execute(state, query, variables, operation_name)
    assert(type(state) == 'table', 'use :gql_execute(...) instead of ' ..
        '.execute(...)')
    assert(state.schema ~= nil, 'have not compiled schema')
    check(query, 'query', 'string')
    check(variables, 'variables', 'table', 'nil')
    check(operation_name, 'operation_name', 'string', 'nil')

    local compiled_query = state:compile(query)
    return compiled_query:execute(variables, operation_name)
end

--- Parse GraphQL query string, validate against the GraphQL schema and
--- provide an object with the function to execute an operation from the
--- request with specific variables values.
---
--- @tparam table state a tarantool_graphql instance
---
--- @tparam string query text of a GraphQL query
---
--- @treturn table compiled query with `execute` and `avro_schema` functions
local function gql_compile(state, query)
    assert(type(state) == 'table' and type(query) == 'string',
        'use :validate(...) instead of .validate(...)')
    assert(state.schema ~= nil, 'have not compiled schema')
    check(query, 'query', 'string')

    local ast = parse(query)
    validate(state.schema, ast)

    local qstate = {
        state = state,
        ast = ast,
    }

    local gql_query = setmetatable(qstate, {
        __index = {
            execute = gql_execute,
            avro_schema = query_to_avro.convert
        }
    })
    return gql_query
end

local function start_server(gql, host, port)
    assert(type(gql) == 'table',
        'use :start_server(...) instead of .start_server(...)')

    check(host, 'host', 'nil', 'string')
    check(port, 'port', 'nil', 'number')

    gql.server = server.init(gql, host, port)
    gql.server:start()

    return ('The GraphQL server started at http://%s:%s'):format(
        gql.server.host, gql.server.port
    )
end

local function stop_server(gql)
    assert(type(gql) == 'table',
        'use :stop_server(...) instead of .stop_server(...)')
    assert(gql.server, 'no running server to stop')

    gql.server:stop()

    return ('The GraphQL server stopped at http://%s:%s'):format(
        gql.server.host, gql.server.port)
end

function tarantool_graphql.compile(query)
    if default_instance == nil then
        default_instance = tarantool_graphql.new()
    end
    return default_instance:compile(query)
end

function tarantool_graphql.execute(query, variables, operation_name)
    if default_instance == nil then
        default_instance = tarantool_graphql.new()
    end
    return default_instance:execute(query, variables, operation_name)
end

function tarantool_graphql.start_server()
    if default_instance == nil then
        default_instance = tarantool_graphql.new()
    end

    return default_instance:start_server()
end

function tarantool_graphql.stop_server()
    if default_instance ~= nil and default_instance.server ~= nil then
        return default_instance:stop_server()
    end
    return 'there is no active server in default Tarantool graphql instance'
end

--- The function creates an accessor of desired type with default configuration.
---
--- @tparam table cfg general tarantool_graphql config (contains schemas,
--- collections, service_fields and indexes)
--- @tparam string accessor type of desired accessor (space or shard)
--- @tparam table accessor_funcs set of functions to overwrite accessor
--- inner functions (`is_collection_exists`, `get_index`, `get_primary_index`,
--- `unflatten_tuple`, For more detailed description see @{accessor_general.new})
--- These function allow this abstract data accessor behaves in the certain way.
--- Note that accessor_space and accessor_shard have their own set of these functions
--- and accessorFuncs argument (if passed) will be used to overwrite them
local function create_default_accessor(cfg)
    check(cfg.accessor, 'cfg.accessor', 'string')
    assert(cfg.accessor == 'space' or cfg.accessor == 'shard',
        'accessor_type must be shard or space, got ' .. cfg.accessor)
    check(cfg.service_fields, 'cfg.service_fields', 'table')
    check(cfg.indexes, 'cfg.indexes', 'table')
    check(cfg.collection_use_tomap, 'cfg.collection_use_tomap', 'table', 'nil')
    check(cfg.accessor_funcs, 'cfg.accessor_funcs', 'table', 'nil')

    if cfg.accessor == 'space' then
        return accessor_space.new({
            schemas = cfg.schemas,
            collections = cfg.collections,
            service_fields = cfg.service_fields,
            indexes = cfg.indexes,
            collection_use_tomap = cfg.collection_use_tomap,
            resulting_object_cnt_max = cfg.resulting_object_cnt_max,
            fetched_object_cnt_max = cfg.fetched_object_cnt_max,
            timeout_ms = cfg.timeout_ms,
            enable_mutations = cfg.enable_mutations,
        }, cfg.accessor_funcs)
    end

    if cfg.accessor == 'shard' then
        return accessor_shard.new({
            schemas = cfg.schemas,
            collections = cfg.collections,
            service_fields = cfg.service_fields,
            indexes = cfg.indexes,
            collection_use_tomap = cfg.collection_use_tomap,
            resulting_object_cnt_max = cfg.resulting_object_cnt_max,
            fetched_object_cnt_max = cfg.fetched_object_cnt_max,
            timeout_ms = cfg.timeout_ms,
            enable_mutations = cfg.enable_mutations,
        }, cfg.accessor_funcs)
    end
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
---                 -- * from has the following structure:
---                 --
---                 -- {
---                 --     collection_name = <...>,
---                 --     connection_name = <...>,
---                 --     destination_args_names = <...>,
---                 --     destination_args_values = <...>,
---                 -- }
---                 --
---                 -- from.collection_name is nil for a top-level collection.
---                 --
---                 -- `extra` is a table which contains additional data for
---                 -- the query:
---                 --
---                 -- * `qcontext` (table) can be used by an accessor to store
---                 --   any query-related data;
---                 -- * `resolveField(field_name, object, filter, opts)`
---                 --   (function) for performing a subrequest on a fields
---                 --   connected using a 1:1 or 1:1* connection.
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
---             extra_args = function(self, collection_name)
---                 ...
---                 local args_meta = {
---                     arg_name = {
---                         add_to_mutations_only = true / false,
---                         add_to_top_fields_only = true / false,
---                     }
---                 }
---                 return schemas_list, args_meta
---             end
---         }
---     }),
--- })
function tarantool_graphql.new(cfg)
    local cfg = cfg or {}

    -- auto config case
    if not next(cfg) or utils.has_only(cfg, 'connections') then
        local generated_cfg = simple_config.graphql_cfg_from_tarantool()
        generated_cfg.accessor = 'space'
        generated_cfg.connections = cfg.connections or {}
        cfg = generated_cfg
        cfg = config_complement.complement_cfg(cfg)
    end

    check(cfg.accessor, 'cfg.accessor', 'string', 'table')
    if type(cfg.accessor) == 'string' then
        cfg.accessor = create_default_accessor(cfg)
    end

    local state = parse_cfg(cfg)
    return setmetatable(state, {
        __index = {
            compile = gql_compile,
            execute = compile_and_execute,
            start_server = start_server,
            stop_server = stop_server,
            internal = { -- for unit testing
                cfg = cfg,
            }
        }
    })
end

return tarantool_graphql

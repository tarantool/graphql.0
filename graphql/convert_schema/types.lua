--- Convert avro-schema types to GraphQL types and set resolve functions.

local json = require('json')
local yaml = require('yaml')
local core_types = require('graphql.core.types')
local avro_helpers = require('graphql.avro_helpers')
local core_types_helpers = require('graphql.convert_schema.core_types_helpers')
local scalar_types = require('graphql.convert_schema.scalar_types')
local resolve = require('graphql.convert_schema.resolve')
local helpers = require('graphql.convert_schema.helpers')

local utils = require('graphql.utils')
local check = utils.check

local types = {}

--- Convert each field of an avro-schema to a graphql type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.nullable_collection_types
---
--- @tparam table fields fields part from an avro-schema
---
--- @tparam table context as described in @{types.convert}; not used here
--- explicitly, but `path` and `field_name` are *updated* and the `context` is
--- passed deeper within the @{types.convert} call
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
            kind = types.convert(state, field.type, {context = context}),
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
        return core_types.nonNull(destination_type)
    elseif connection_type == '1:1*' then
        return destination_type
    elseif connection_type == '1:N' then
        return core_types.nonNull(core_types.list(core_types.nonNull(
            destination_type)))
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
local function box_collection_type(type_to_box, connection_type,
        type_to_box_name)
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

    -- box_type_name seen in 'on' clause, so we cannot use full name here.
    -- XXX: Ideally we should deduplicate it using defined names set (graphql
    --      schema / db_schema local) and autorenaming with ..._2, ..._3.
    local field_name = type_to_box_name
    local box_field = {
        [field_name] = {
            name = field_name,
            kind = type_to_box,
        }
    }
    local box_type = core_types.object({
        name = box_type_name,
        description = box_type_description,
        fields = box_field
    })

    return box_type, field_name
end

--- The function converts passed simple connection to a field of GraphQL type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.nullable_collection_types (those are gql types)
---
--- @tparam table connection simple connection to create field
---
--- @tparam table collection_name name of the collection which has given
--- connection
---
--- @treturn table generated field
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

    destination_type = specify_destination_type(destination_type, c.type)

    local c_args = args_from_destination_collection(state,
        c.destination_collection, c.type)
    local c_list_args = state.list_arguments[c.destination_collection]
    local e_args = state.extra_arguments[c.destination_collection]

    local arguments = {
        all = c_args,
        list = c_list_args,
        extra = e_args,
    }

    local resolve_function = resolve.gen_resolve_function(collection_name, c,
        destination_type, arguments, state.accessor)

    local field = {
        name = c.name,
        kind = destination_type,
        arguments = c_args,
        resolve = resolve_function,
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
---
--- @tparam table connection multi-head connection to create field
---
--- @tparam table collection_name name of the collection which has given
--- connection
---
--- @tparam table context avro-schema parsing context as described in
--- @{types.convert}
---
--- @treturn table generated field
local function convert_multihead_connection(state, connection, collection_name,
        context)
    local c = connection
    local union_types = {}
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
    end

    local resolve_function = resolve.gen_resolve_function_multihead(
        collection_name, c, union_types, var_num_to_box_field_name,
        state.accessor)

    local field = {
        name = c.name,
        kind = core_types.union({
            name = helpers.full_name(c.name, context),
            types = union_types,
        }),
        arguments = nil, -- see Border cases/Unions at the top of
                         -- tarantool_graphql module description
        resolve = resolve_function,
    }
    return field
end

--- The function converts passed connection to a field of GraphQL type.
---
--- @tparam table state for read state.accessor and previously filled
--- state.types (state.types are gql types)
---
--- @tparam table connection connection to create field
---
--- @tparam table collection_name name of the collection which have given
--- connection
---
--- @tparam table context avro-schema parsing context as described in
--- @{types.convert}
---
--- @treturn table generated field
local convert_connection_to_field = function(state, connection, collection_name,
        context)
    check(connection.type, 'connection.type', 'string')
    assert(connection.type == '1:1' or connection.type == '1:1*' or
        connection.type == '1:N', 'connection.type must be 1:1, 1:1* or 1:N, '..
        'got ' .. connection.type)
    check(connection.name, 'connection.name', 'string')
    assert(connection.destination_collection or connection.variants,
        'connection must either destination_collection or variants field')
    check(context, 'context', 'table')

    if connection.destination_collection then
        return convert_simple_connection(state, connection, collection_name)
    end

    if connection.variants then
        return convert_multihead_connection(state, connection, collection_name,
            context)
    end
end

--- The function 'boxes' given GraphQL type into GraphQL Object 'box' type.
---
--- @tparam table type_to_box GraphQL type to be boxed
---
--- @tparam string box_field_name name of the single box field
---
--- @tparam table context avro-schema parsing context as described in
--- @{types.convert}
---
--- @treturn table GraphQL Object
local function box_type(type_to_box, box_field_name, context)
    check(type_to_box, 'type_to_box', 'table')
    check(box_field_name, 'box_field_name', 'string')

    local gql_true_type = core_types_helpers.nullable(type_to_box)

    -- We cannot use full name for a GraphQL union variant ('boxed type'),
    -- because it is seen by a user in 'on' clause. See also the comment in
    -- box_collection_type().
    local box_name = gql_true_type.name or gql_true_type.__type
    box_name = helpers.base_name(box_name .. '_box')

    local box_fields = {
        [box_field_name] = {
            name = box_field_name,
            kind = type_to_box,
        }
    }

    return core_types.object({
        name = box_name,
        description = 'Box (wrapper) around union variant',
        fields = box_fields,
    })
end

--- The functions creates table of GraphQL types from avro-schema union type.
---
--- @tparam table avro_schema
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table context as described in @{types.convert}; not used here
--- explicitly, but passed deeper within the @{types.convert} call
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
            local variant_type = types.convert(state, type,
                {context = context})
            local box_field_name = type.name or avro_helpers.avro_type(type)
            union_types[#union_types + 1] = box_type(variant_type,
                box_field_name, context)
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
--- (see @{types.convert})
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
--- @tparam table context as described in @{types.convert}; only
--- `context.field_name` is used here (as the name of the generated GraphQL
--- union); `path` is *updated* (with the field name) and the `context` is
--- passed deeper within the @{create_union_types} call (which calls
--- @{types.convert} inside)
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
        assert(avro_helpers.avro_type(type) ~= 'union',
            'unions must not immediately contain other unions')

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
                    assert(avro_helpers.avro_type(type) ~=
                        avro_helpers.avro_type(another_type),
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

    local union_type = core_types.union({
        types = union_types,
        name = helpers.full_name(union_name, context),
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
        union_type = core_types.nonNull(union_type)
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
--- @tparam[opt] table opts the following options:
---
--- * `collection` (table; optional) when passed it will be used to generate
---   fields for connections
---
--- * `type_name` (string; optional) when passed it will be used to generate
---    name of the GraphQL type instead of one from avro_schema (considered
---    only for record / record*)
---
--- * context (table; optional) current context of parsing the avro_schema,
---   consists the following fields:
---
---   - `field_name` (string; optional) it is only for an union generation,
---     because avro-schema union has no name in it and specific name is
---     necessary for GraphQL union
---
---   - `path` (table) path to our position in avro-schema tree; used in
---      GraphQL types names generation
---
--- Note: map is considered scalar. This means that particular fields cannot be
--- requested using GraphQL, only the entire map or nothing.
function types.convert(state, avro_schema, opts)
    check(state, 'state', 'table')
    check(avro_schema, 'avro_schema', 'table', 'string')
    check(opts, 'opts', 'table', 'nil')

    local opts = opts or {}
    local collection = opts.collection
    local type_name = opts.type_name
    local context = opts.context
    if context == nil then
        context = {
            field_name = nil,
            path = {},
        }
    end

    check(collection, 'collection', 'table', 'nil')
    check(type_name, 'type_name', 'string', 'nil')
    check(context, 'context', 'table')

    local field_name = context.field_name
    local path = context.path

    check(field_name, 'field_name', 'string', 'nil')
    check(path, 'path', 'table')

    local accessor = state.accessor
    check(accessor, 'accessor', 'table')
    check(accessor.select, 'accessor.select', 'function')

    local avro_t = avro_helpers.avro_type(avro_schema)

    if avro_t == 'record' or avro_t == 'record*' then
        if type(avro_schema.name) ~= 'string' then -- avoid extra json.encode()
            assert(type(avro_schema.name) == 'string',
                ('avro_schema.name must be a string, got %s (avro_schema %s)')
                :format(type(avro_schema.name), json.encode(avro_schema)))
        end
        if type(avro_schema.fields) ~= 'table' then -- avoid extra json.encode()
            assert(type(avro_schema.fields) == 'table',
                ('avro_schema.fields must be a table, got %s (avro_schema %s)')
                :format(type(avro_schema.fields), json.encode(avro_schema)))
        end

        local type_name = type_name or avro_schema.name

        table.insert(context.path, type_name)
        local fields = convert_record_fields(state, avro_schema.fields, context)
        table.remove(context.path, #context.path)

        -- if collection param is passed then go over all connections
        for _, c in ipairs((collection or {}).connections or {}) do
            fields[c.name] = convert_connection_to_field(state, c,
                collection.name, context)
        end

        -- create GraphQL type
        local res = core_types.object({
            name = helpers.full_name(type_name, context),
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        })
        return avro_t == 'record' and core_types.nonNull(res) or res
    elseif avro_t == 'enum' then
        error('enums not implemented yet') -- XXX
    elseif avro_t == 'array' or avro_t == 'array*' then
        assert(avro_schema.items ~= nil,
            'items field must not be nil in array avro schema')
        assert(type(avro_schema.items) == 'string'
            or type(avro_schema.items) == 'table',
            'avro_schema.items must be a string or a table, got ' ..
            type(avro_schema.items))

        local gql_items_type = types.convert(state, avro_schema.items,
            {context = context})
        local res = core_types.list(gql_items_type)
        return avro_t == 'array' and core_types.nonNull(res) or res
    elseif avro_t == 'map' or avro_t == 'map*' then
        assert(avro_schema.values ~= nil,
            'values must not be nil in map avro schema')
        assert(type(avro_schema.values) == 'table'
            or type(avro_schema.values) == 'string',
            ('avro_schema.values must be a table or a string, ' ..
            'got %s (avro_schema %s)'):format(type(avro_schema.values),
            json.encode(avro_schema)))

        -- validate avro schema format inside 'values'
        types.convert(state, avro_schema.values, {context = context})

        local res = core_types.map
        return avro_t == 'map' and core_types.nonNull(res) or res
    elseif avro_t == 'union' then
        return create_gql_union(state, avro_schema, context)
    else
        local res = scalar_types.convert(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' ..
                json.encode(avro_schema))
        end
        return res
    end
end

return types

local yaml = require('yaml')
local core_types = require('graphql.core.types')
local avro_helpers = require('graphql.avro_helpers')
local helpers = require('graphql.convert_schema.helpers')

local utils = require('graphql.utils')
local check = utils.check

local union = {}

--- The function 'boxes' given GraphQL type into GraphQL Object 'box' type.
---
--- @tparam table type_to_box GraphQL type to be boxed
---
--- @tparam string box_field_name name of the single box field
---
--- @tparam table opts the following options:
---
--- * gen_argument (boolean) whether resulting type will be used in result
---   types (false) or argument types (true)
---
--- * context (table) avro-schema parsing context as described in
---   @{types.convert} and @{arguments.convert}
---
--- @treturn table GraphQL Object
local function box_type(type_to_box, box_field_name, opts)
    check(type_to_box, 'type_to_box', 'table')
    check(box_field_name, 'box_field_name', 'string')

    local gen_argument = opts.gen_argument
    local context = opts.context

    local gql_true_type = core_types.nullable(type_to_box)

    -- Use bare name for the result type (to use in 'on' clause) and full name
    -- for the argument type to avoid 'Encountered multiple types' error. See
    -- also the comment in @{types.box_collection_type}.
    local box_name = (gql_true_type.name or gql_true_type.__type) .. '_box'
    if gen_argument then
        box_name = helpers.full_name(box_name, context)
    else
        box_name = helpers.base_name(box_name)
    end

    local box_fields = {
        [box_field_name] = {
            name = box_field_name,
            kind = type_to_box,
        }
    }

    local box_object_type = gen_argument and core_types.inputObject or
        core_types.object

    return box_object_type({
        name = box_name,
        description = 'Box (wrapper) around union variant',
        fields = box_fields,
    })
end

--- The functions creates table of GraphQL types from avro-schema union type.
---
--- @tparam table avro_schema
---
--- @tparam table opts the following options:
---
--- * convert (function) @{types.convert} or @{arguments.convert}
---
--- * gen_argument (boolean) whether resulting type will be used in result
---   types (false) or argument types (true)
---
--- * context (table) as described in @{types.convert} and
---   @{arguments.convert}; not used here explicitly, but passed deeper within
---   the @{types.convert} or @{arguments.convert} call
---
--- @treturn table union_types
---
--- @treturn table determinant_to_type
---
--- @treturn boolean is_nullable
local function create_union_types(avro_schema, opts)
    check(avro_schema, 'avro_schema', 'table')
    assert(utils.is_array(avro_schema), 'union avro-schema must be an array ' ..
        ', got\n' .. yaml.encode(avro_schema))

    local convert = opts.convert
    local gen_argument = opts.gen_argument
    local context = opts.context

    local union_types = {}
    local determinant_to_type = {}
    local is_nullable = false

    for _, type in ipairs(avro_schema) do
        -- If there is a 'null' type among 'union' types (in avro-schema union)
        -- then resulting GraphQL Union type will be nullable
        if type == 'null' then
            is_nullable = true
        else
            local box_field_name = type.name or avro_helpers.avro_type(type)
            table.insert(context.path, box_field_name)
            local variant_type = convert(type, {context = context})
            table.remove(context.path, #context.path)
            union_types[#union_types + 1] = box_type(variant_type,
                box_field_name, {
                    gen_argument = gen_argument,
                    context = context,
                })
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
--- Avro scalar types (e.g. int, string) are converted into GraphQL Object or
--- InputObject types. Avro scalar converted to GraphQL Scalar (string ->
--- String) and then name of GraphQL type is concatenated with '_box'
--- ('String_box'). Resulting name is a name of created GraphQL Object /
--- InputObject. This object has only one field with GraphQL type
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
--- Avro arrays and maps are converted into GraphQL Object or InputObject
--- types. The name of the resulting GraphQL Object is 'List_box' or 'Map_box'
--- respectively. This object has only one field with GraphQL type
--- corresponding to 'items' / 'values' avro type. 'array' or 'map'
--- (respectively) is taken as a name of this single field.
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
--- Avro records are converted into GraphQL Object or InputObject types. The
--- name of the resulting GraphQL Object is concatenation of record's name and
--- '_box'. This Object has only one field. The name of this field is record's
--- name. The type of this field is GraphQL Object / InputObject generated from
--- avro record schema in a usual way (see @{types.convert} and
--- @{arguments.convert})
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
--- @tparam table avro_schema avro-schema union type
---
--- @tparam table opts the following options:
---
--- * convert (function) @{types.convert} or @{arguments.convert}
---
--- * gen_argument (boolean) whether resulting type will be used in result
--- types (false) or argument types (true)
---
--- * context (table) as described in @{types.convert} and
---   @{arguments.convert}; only `context.field_name` is used here (as the name
---   of the generated GraphQL union); `path` is *updated* (with the field
---   name) and the `context` is passed deeper within the @{create_union_types}
---   call (which calls @{types.convert} or @{arguments.convert} inside)
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
function union.convert(avro_schema, opts)
    check(avro_schema, 'avro_schema', 'table')
    assert(utils.is_array(avro_schema), 'union avro-schema must be an ' ..
        'array, got:\n' .. yaml.encode(avro_schema))

    local opts = opts or {}
    check(opts, 'opts', 'table')

    local convert = opts.convert
    local gen_argument = opts.gen_argument or false
    local context = opts.context

    check(convert, 'convert', 'function')
    check(gen_argument, 'gen_argument', 'boolean')
    check(context, 'context', 'table')

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
        create_union_types(avro_schema, {
            convert = convert,
            gen_argument = gen_argument,
            context = context,
        })
    table.remove(context.path, #context.path)

    local union_constructor = gen_argument and core_types.inputUnion or
        core_types.union

    local union_type = union_constructor({
        types = union_types,
        name = helpers.full_name(union_name, context),
        resolveType = function(result)
            assert(type(result) == 'table',
                'union value must be a map with one field, got ' ..
                type(result))
            assert(next(result) ~= nil and next(result, next(result)) == nil,
                'union value must have only one field')
            for determinant, type in pairs(determinant_to_type) do
                if result[determinant] ~= nil then
                    return type
                end
            end
            local field_name = tostring(next(result))
            error(('unexpected union value field: %s'):format(field_name))
        end,
        resolveNodeType = function(node)
            assert(#node.values == 1,
                ('box object with more then one field: %d'):format(
                #node.values))
            local determinant = node.values[1].name
            check(determinant, 'determinant', 'string')
            local res = determinant_to_type[determinant]
            assert(determinant ~= nil,
                ('the union has no "%s" field'):format(determinant))
            return res
        end,
    })

    if not is_nullable then
        union_type = core_types.nonNull(union_type)
    end

    return union_type
end

return union

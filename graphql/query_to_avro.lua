--- Module for convertion GraphQL query to Avro schema.
---
--- Random notes:
---
--- * The best way to use this module is to just call `avro_schema` method on
---   compiled query object.

local path = "graphql.core"
local introspection = require(path .. '.introspection')
local query_util = require(path .. '.query_util')
local avro_helpers = require('graphql.avro_helpers')
local convert_schema_helpers = require('graphql.convert_schema.helpers')
local utils = require('graphql.utils')
local check = utils.check

-- module functions
local query_to_avro = {}

-- forward declaration
local object_to_avro
local map_to_avro
local union_to_avro

local gql_scalar_to_avro_index = {
    String = "string",
    Int = "int",
    Long = "long",
    -- GraphQL Float is double precision according to graphql.org.
    -- More info http://graphql.org/learn/schema/#scalar-types
    Float = "double",
    Boolean = "boolean"
}

local function gql_scalar_to_avro(fieldType)
    assert(fieldType.__type == "Scalar", "GraphQL scalar field expected")
    if fieldType.subtype == "Map" then
        return map_to_avro(fieldType)
    end
    local result = gql_scalar_to_avro_index[fieldType.name]
    assert(result ~= nil, "Unexpected scalar type: " .. fieldType.name)
    return result
end

--- Convert GraphQL type to avro-schema with selecting fields.
---
--- @tparam table fieldType GraphQL type
---
--- @tparam table subSelections fields to select from resulting avro-schema
--- (internal graphql-lua format)
---
--- @tparam table context current traversal context, here it just falls to the
--- called functions (internal graphql-lua format)
---
--- @treturn table `result` is the resulting avro-schema
local function gql_type_to_avro(fieldType, subSelections, context)
    local fieldTypeName = fieldType.__type
    local isNonNull = false

    -- In case the field is NonNull, the real type is in ofType attribute.
    while fieldTypeName == 'NonNull' do
        fieldType = fieldType.ofType
        fieldTypeName = fieldType.__type
        isNonNull = true
    end

    local result

    if fieldTypeName == 'List' then
        local innerType = fieldType.ofType
        local innerTypeAvro = gql_type_to_avro(innerType, subSelections,
            context)
        result = {
            type = "array",
            items = innerTypeAvro,
        }
    elseif fieldTypeName == 'Scalar' then
        result = gql_scalar_to_avro(fieldType)
    elseif fieldTypeName == 'Object' then
        result = object_to_avro(fieldType, subSelections, context)
    elseif fieldTypeName == 'Union' then
        result = union_to_avro(fieldType, subSelections, context)
    elseif fieldTypeName == 'Interface' then
        error('Interfaces are not supported yet')
    else
        error(string.format('Unknown type "%s"', tostring(fieldTypeName)))
    end

    if not isNonNull then
        result = avro_helpers.make_avro_type_nullable(result, {
            raise_on_nullable = true,
        })
    end
    return result
end

--- The function converts a GraphQL Map type to avro-schema map type.
map_to_avro = function(mapType)
    assert(mapType.values ~= nil, "GraphQL Map type must have 'values' field")
    return {
        type = "map",
        values = gql_type_to_avro(mapType.values),
    }
end

--- Converts a GraphQL Union type to avro-schema type.
---
--- Currently we use GraphQL Unions to implement both multi-head connections
--- and avro-schema unions. The function distinguishes between them relying on
--- 'fieldType.resolveType'. GraphQL Union implementing multi-head
--- connection does not have such field, as it has another mechanism of union
--- type resolving.
---
--- We have to distinguish between these two types of GraphQL Unions because
--- we want to create different avro-schemas for them.
---
--- GraphQL Unions implementing avro-schema unions are to be converted back
--- to avro-schema unions.
---
--- GraphQL Unions implementing multi-head connections are to be converted to
--- avro-schema records. Each field represents one union variant. Variant type
--- name is taken as a field name. Such records must have all fields nullable.
---
--- We convert Unions implementing multi-head connections to records instead of
--- unions because in case of 1:N connections we would not have valid
--- avro-schema (if use unions). Avro-schema unions may not contain more than
--- one schema with the same non-named type (in case of 1:N multi-head
--- connections we would have more than one 'array' in union).
union_to_avro = function(fieldType, subSelections, context)
    assert(fieldType.types ~= nil, "GraphQL Union must have 'types' field")
    check(fieldType.types, "fieldType.types", "table")
    local is_multihead = (fieldType.resolveType == nil)
    local result

    if is_multihead then
        check(fieldType.name, "fieldType.name", "string")
        result = {
            type = 'record',
            name = fieldType.name,
            fields = {}
        }
    else
        result = {}
    end

    for _, box_type in ipairs(fieldType.types) do
        -- In GraphQL schema all types in Unions are 'boxed'. Here we
        -- 'Unbox' types and selectionSets. More info on 'boxing' can be
        -- found at @{convert_schema.types.convert_multihead_connection}
        -- and at @{convert_schema.union}.
        check(box_type, "box_type", "table")
        assert(box_type.__type == "Object", "Box type must be a GraphQL Object")
        assert(utils.table_size(box_type.fields) == 1, 'Box Object must ' ..
            'have exactly one field')
        local type = select(2, next(box_type.fields))

        local box_sub_selections
        for _, s in pairs(subSelections) do
            if s.typeCondition.name.value == box_type.name then
                box_sub_selections = s
                break
            end
        end

        -- Skip Union variants that are not parts of the query.
        if box_sub_selections == nil then
            goto continue
        end

        -- We have to extract subSelections from 'box' type.
        local type_sub_selections
        if box_sub_selections.selectionSet.selections[1].selectionSet ~= nil then
            -- Object GraphQL type case.
            type_sub_selections = box_sub_selections.selectionSet
                .selections[1].selectionSet.selections
        else
            -- Scalar GraphQL type case.
            type_sub_selections = box_sub_selections.selectionSet.selections[1]
        end
        assert(type_sub_selections ~= nil)

        if is_multihead then
            local avro_type = gql_type_to_avro(type.kind,
                type_sub_selections, context)
            avro_type = avro_helpers.make_avro_type_nullable(avro_type)
            table.insert(result.fields, {name = type.name, type = avro_type})
        else
            table.insert(result, gql_type_to_avro(type.kind,
                type_sub_selections, context))
        end

        ::continue::
    end

    return result
end

--- The function converts a single Object field to avro format.
local function field_to_avro(object_type, fields, context)
    local firstField = fields[1]
    assert(#fields == 1, "The aliases are not considered yet")
    local fieldName = firstField.name.value
    local fieldType = introspection.fieldMap[fieldName] or
        object_type.fields[fieldName]
    assert(fieldType ~= nil)
    local subSelections = query_util.mergeSelectionSets(fields)

    local fieldTypeAvro = gql_type_to_avro(fieldType.kind, subSelections,
        context)
    -- Currently we support only 'include' and 'skip' directives. Both of them
    -- affect resulting avro-schema the same way: field with directive becomes
    -- nullable, if it's already not. Nullable field does not change.
    --
    -- If it is a 1:N connection then it's 'array' field becomes 'array*'.
    -- If it is avro-schema union, then 'null' will be added to the union
    -- types. If there are more then one directive on a field then all works
    -- the same way, like it is only one directive. (But we still check all
    -- directives to be 'include' or 'skip').
    if firstField.directives ~= nil then
        for _, d in ipairs(firstField.directives) do
            check(d.name, "directive.name", "table")
            check(d.arguments, "directive.arguments", "table")
            check(d.kind, "directive.kind", "string")
            assert(d.kind == "directive")
            check(d.name.value, "directive.name.value", "string")
            assert(d.name.value == "include" or d.name.value == "skip",
                "Only 'include' and 'skip' directives are supported for now")
        end
        fieldTypeAvro = avro_helpers.make_avro_type_nullable(fieldTypeAvro)
    end

    return {
        name = convert_schema_helpers.base_name(fieldName),
        type = fieldTypeAvro,
    }
end

--- Convert GraphQL object to avro record.
---
--- @tparam table object_type GraphQL type object to be converted to Avro schema
---
--- @tparam table selections GraphQL representations of fields which should be
--- in the output of the query
---
--- @tparam table context additional information for Avro schema generation; one
--- of the fields is `namespace_parts` -- table of names of records from the
--- root to the current object
---
--- @treturn table `result` is the corresponding Avro schema
object_to_avro = function(object_type, selections, context)
    local fields = query_util.collectFields(object_type, selections,
        {}, {}, context)
    local result = {
        type = 'record',
        name = convert_schema_helpers.base_name(object_type.name),
        fields = {}
    }
    if #context.namespace_parts ~= 0 then
        result.namespace = table.concat(context.namespace_parts, ".")
    end
    table.insert(context.namespace_parts, result.name)
    for _, field in pairs(fields) do
        local avro_field = field_to_avro(object_type, {field.selection},
            context)
        table.insert(result.fields, avro_field)
    end
    context.namespace_parts[#context.namespace_parts] = nil
    return result
end

--- Create an Avro schema for a given query / operation.
---
--- @tparam table qstate compiled query for which the avro schema should be
--- created
---
--- @tparam[opt] string operation_name optional operation name
---
--- @treturn table `avro_schema` avro schema for any
--- `qstate:execute(..., operation_name)` result
function query_to_avro.convert(qstate, operation_name)
    assert(type(qstate) == "table",
        ('qstate should be a table, got: %s; ' ..
        'hint: use ":" instead of "."'):format(type(table)))
    local state = qstate.state
    local context = query_util.buildContext(state.schema, qstate.ast, {}, {},
        operation_name)
    -- The variable is necessary to avoid fullname interferention.
    -- Each nested Avro record creates it's namespace.
    context.namespace_parts = {}
    local rootType = state.schema[context.operation.operation]
    local selections = context.operation.selectionSet.selections
    return object_to_avro(rootType, selections, context)
end

return query_to_avro

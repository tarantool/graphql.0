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

-- module functions
local query_to_avro = {}

-- forward declaration
local object_to_avro
local map_to_avro

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
    elseif fieldTypeName == 'Interface' or fieldTypeName == 'Union' then
        error('Interfaces and Unions are not supported yet')
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

--- Module for convertion GraphQL query to Avro schema.
---
--- Random notes:
---
--- * The best way to use this module is to just call `avro_schema` methon on
---   compiled query object.
local path = "graphql.core"
local introspection = require(path .. '.introspection')
local query_util = require(path .. '.query_util')

-- module functions
local query_to_avro = {}

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
    assert(fieldType.name ~= "Map", "Map type is not supported")
    local result = gql_scalar_to_avro_index[fieldType.name]
    assert(result ~= nil, "Unexpected scalar type: " .. fieldType.name)
    return result
end

-- The function converts avro type to nullable.
-- In current tarantool/avro-schema implementation we simply add '*'
-- to the end of type name.
-- The function do not copy the resulting type but changes it in place.
--
-- @tparam table avro schema node to be converted to nullable
--
-- @tresult table schema node; basically it is the passed schema node,
-- however in nullable type implementation through unions it can be different
-- node
local function make_avro_type_nullable(avro)
    assert(avro.type ~= nil, "Avro `type` field is necessary")
    local type_type = type(avro.type)
    if type_type == "string" then
        assert(avro.type:endswith("*") == false,
            "Avro type should not be nullable already")
        avro.type = avro.type .. '*'
        return avro
    end
    if type_type == "table" then
        avro.type = make_avro_type_nullable(avro.type)
        return avro
    end
    error("Avro type should be a string or table, got :" .. type_type)
end

local object_to_avro

local function complete_field_to_avro(fieldType, result, subSelections, context,
        NonNull)
    local fieldTypeName = fieldType.__type
    if fieldTypeName == 'NonNull' then
        -- In case the field is NonNull, the real type is in ofType attribute.
        fieldType = fieldType.ofType
        fieldTypeName = fieldType.__type
    elseif NonNull ~= true then
        -- Call complete_field second time and make result nullable.
        result = complete_field_to_avro(fieldType, result, subSelections,
            context, true)
        result = make_avro_type_nullable(result)
        return result
    end

    if fieldTypeName == 'List' then
        local innerType = fieldType.ofType
        -- Steal type from virtual object.
        -- This is necessary because in case of arrays type should be
        -- "completed" into results `items` field, but in other cases (Object,
        -- Scalar) it should be completed into `type` field.
        local items = complete_field_to_avro(innerType, {}, subSelections,
            context).type
        result.type = {
            type = "array",
            items = items
        }
        return result
    end

    if fieldTypeName == 'Scalar' then
        result.type = gql_scalar_to_avro(fieldType)
        return result
    end

    if fieldTypeName == 'Object' then
        result.type = object_to_avro(fieldType, subSelections, context)
        return result
    elseif fieldTypeName == 'Interface' or fieldTypeName == 'Union' then
        error('Interfaces and Unions are not supported yet')
    end
    error(string.format('Unknown type "%s"', fieldTypeName))
end

--- The function converts a single Object field to avro format
local function field_to_avro(object_type, fields, context)
    local firstField = fields[1]
    assert(#fields == 1, "The aliases are not considered yet")
    local fieldName = firstField.name.value
    local fieldType = introspection.fieldMap[fieldName] or
        object_type.fields[fieldName]
    assert(fieldType ~= nil)
    local subSelections = query_util.mergeSelectionSets(fields)
    local result = {}
    result.name = fieldName
    result = complete_field_to_avro(fieldType.kind, result, subSelections,
        context)
    return result
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
--- @treturn table corresponding Avro schema
object_to_avro = function(object_type, selections, context)
    local groupedFieldSet = query_util.collectFields(object_type, selections,
        {}, {}, context)
    local result = {
        type = 'record',
        name = object_type.name,
        fields = {}
    }
    if #context.namespace_parts ~= 0 then
        result.namespace = table.concat(context.namespace_parts, ".")
    end
    table.insert(context.namespace_parts, result.name)
    for _, fields in pairs(groupedFieldSet) do
        local avro_field = field_to_avro(object_type, fields, context)
        table.insert(result.fields, avro_field)
    end
    context.namespace_parts[#context.namespace_parts] = nil
    return result
end

--- Create an Avro schema for a given query.
---
--- @tparam table query object which avro schema should be created for
---
--- @treturn table `avro_schema` avro schema for any `query:execute()` result.
function query_to_avro.convert(query)
    assert(type(query) == "table",
        'query should be a table, got: ' .. type(table)
        .. '; hint: use ":" instead of "."')
    local state = query.state
    local context = query_util.buildContext(state.schema, query.ast, {}, {},
        query.operation_name)
    -- The variable is necessary to avoid fullname interferention.
    -- Each nested Avro record creates it's namespace.
    context.namespace_parts = {}
    local rootType = state.schema[context.operation.operation]
    local selections = context.operation.selectionSet.selections
    return object_to_avro(rootType, selections, context)
end

return query_to_avro

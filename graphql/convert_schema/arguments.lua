--- Convert avro-schema fields to GraphQL arguments (scalars and InputObjects).

local json = require('json')
local core_types = require('graphql.core.types')
local avro_helpers = require('graphql.avro_helpers')
local scalar_types = require('graphql.convert_schema.scalar_types')
local helpers = require('graphql.convert_schema.helpers')
local union = require('graphql.convert_schema.union')

local utils = require('graphql.utils')
local check = utils.check

local arguments = {}

--- Convert avro-schema type to GraphQL scalar or InputObject.
---
--- @param avro_schema (table or string) avro-schema with expanded references
---
--- @tparam table opts the following options:
---
--- * type_name (string; optional) name for GraphQL type instead of one from
---   avro-schema full name (considered for record / record*)
---
--- * context (table) avro-schema processing context:
---
---   - field_name (string; optional) name of currently parsed field
---
---   - path (table) point where we are in avro-schema
---
--- @treturn table GraphQL scalar or InputObject
local function convert(avro_schema, opts)
    check(avro_schema, 'avro_schema', 'table', 'string')
    check(opts, 'opts', 'table', 'nil')

    local opts = opts or {}
    local type_name = opts.type_name
    local context = opts.context

    check(type_name, 'type_name', 'string', 'nil')
    check(context, 'context', 'table')

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

        table.insert(context.path, type_name or avro_schema.name)
        local fields = {}
        for _, field in ipairs(avro_schema.fields) do
            if type(field.name) ~= 'string' then -- avoid extra json.encode()
                assert(type(field.name) == 'string',
                    ('field.name must be a string, got %s (schema %s)')
                    :format(type(field.name), json.encode(field)))
            end

            table.insert(context.path, field.name)
            context.field_name = field.name
            local gql_field_type = convert(field.type, {context = context})
            context.field_name = nil
            table.remove(context.path, #context.path)

            fields[field.name] = {
                name = field.name,
                kind = gql_field_type,
            }
        end
        table.remove(context.path, #context.path)

        local res = core_types.inputObject({
            name = type_name or helpers.full_name(avro_schema.name, context),
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        })

        return avro_t == 'record' and core_types.nonNull(res) or res
    elseif avro_t == 'enum' then
        error('enums do not implemented yet') -- XXX
    elseif avro_t == 'array' or avro_t == 'array*' then
        local gql_items_type = convert(avro_schema.items, {context = context})
        local res = core_types.list(gql_items_type)
        return avro_t == 'array' and core_types.nonNull(res) or res
    elseif avro_t == 'map' or avro_t == 'map*' then
        local gql_values_type = convert(avro_schema.values, {context = context})

        local res = core_types.inputMap({
            name = helpers.full_name('InputMap', context),
            values = gql_values_type,
        })
        return avro_t == 'map' and core_types.nonNull(res) or res
    elseif avro_t == 'union' then
        return union.convert(avro_schema, {
            convert = convert,
            gen_argument = true,
            context = context,
        })
    else
        local res = scalar_types.convert(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' ..
                json.encode(avro_schema))
        end
        return res
    end
end

--- Convert each field of an avro-schema record to a scalar graphql type or an
--- input object.
---
--- @tparam table fields list of fields of the avro-schema record fields format
---
--- @tparam string root_name topmost part of namespace
---
--- @treturn table `args` -- map with type names as keys and graphql types as
--- values
function arguments.convert_record_fields(fields, root_name)
    check(fields, 'fields', 'table')

    local context = {
        field_name = nil,
        path = {'$arguments', root_name},
    }

    local args = {}
    for _, field in ipairs(fields) do
        if type(field.name) ~= 'string' then -- avoid extra json.encode()
            assert(type(field.name) == 'string',
                ('field.name must be a string, got %s (schema %s)')
                :format(type(field.name), json.encode(field)))
        end

        -- We preserve a type name of an uppermost InputObject that starts from
        -- the collection name to allow use it for variables.
        local avro_t = avro_helpers.avro_type(field.type)
        local type_name
        if (avro_t == 'record' or avro_t == 'record*') and
                field.type.name:startswith(root_name) then
            type_name = field.type.name
        end

        table.insert(context.path, field.name)
        context.field_name = field.name
        args[field.name] = convert(field.type, {
            context = context,
            type_name = type_name,
        })
        context.field_name = nil
        table.remove(context.path, #context.path)
    end
    return args
end

return arguments

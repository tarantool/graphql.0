--- Convert avro-schema fields to GraphQL arguments (scalars and InputObjects).

local json = require('json')
local log = require('log')
local core_types = require('graphql.core.types')
local avro_helpers = require('graphql.avro_helpers')
local core_types_helpers = require('graphql.convert_schema.core_types_helpers')
local scalar_types = require('graphql.convert_schema.scalar_types')
local helpers = require('graphql.convert_schema.helpers')

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
            local gql_field_type = convert(field.type, {context = context})
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
    else
        local res = scalar_types.convert(avro_schema, {raise = false})
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
--- It uses the @{convert} function to convert each field, then skips fields
--- of record, array and map types and gives the resulting list of
--- converted fields.
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
        path = {'$arguments', root_name},
    }

    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))

        -- We preserve a type name of an uppermost InputObject that starts from
        -- the collection name to allow use it for variables.
        local avro_t = avro_helpers.avro_type(field.type)
        local type_name
        if (avro_t == 'record' or avro_t == 'record*') and
                field.type.name:startswith(root_name) then
            type_name = field.type.name
        end

        -- XXX: remove pcall when all supported types will be supported in
        -- convert()
        table.insert(context.path, field.name)
        local ok, gql_class = pcall(convert, field.type, {
            context = context,
            type_name = type_name,
        })
        table.remove(context.path, #context.path)
        if ok then
            args[field.name] = gql_class
        else
            log.warn(('Cannot add argument "%s": %s'):format(
                field.name, tostring(gql_class)))
        end
    end
    return args
end

return arguments

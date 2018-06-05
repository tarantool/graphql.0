--- Convert avro-schema fields to GraphQL arguments (scalars and InputObjects).

local json = require('json')
local log = require('log')
local core_types = require('graphql.core.types')
local avro_helpers = require('graphql.avro_helpers')
local core_types_helpers = require('graphql.convert_schema.core_types_helpers')
local scalar_types = require('graphql.convert_schema.scalar_types')

local utils = require('graphql.utils')
local check = utils.check

local arguments = {}

--- Convert each field of an avro-schema to a scalar graphql type or an input
--- object.
---
--- It uses the @{arguments.convert} function to convert each field, then skips
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
function arguments.convert_record_fields(fields, opts)
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
        local avro_t = avro_helpers.avro_type(field.type,
            {allow_references = true})
        local add_field = dont_skip or
            avro_helpers.is_comparable_scalar_type(avro_t) or
            (not skip_compound and not avro_helpers.is_scalar_type(avro_t))
        if add_field then
            local ok, gql_class = pcall(arguments.convert, field.type)
            -- XXX: we need better avro-schema -> graphql types converter to
            -- handle the following cases:
            -- * scalar arguments that can be checked for equality (object
            --   args): skip any other
            -- * pcre / limit / offset (nothing special here I guess)
            -- * auxiliary schemas for insert / update: don't skip anything
            if ok then
                args[field.name] = core_types_helpers.nullable(gql_class)
            else
                log.warn(('Cannot add argument "%s": %s'):format(
                    field.name, tostring(gql_class)))
            end
        end
    end
    return args
end

--- Convert avro-schema type to GraphQL scalar or InputObject.
---
--- An error will be raised if avro_schema type is 'record'
--- and its' fields have non-scalar types. So triple nesting level is not
--- supported (record with record as a field - ok, record with record which
--- has inside an another level - not ok).
function arguments.convert(avro_schema)
    assert(avro_schema ~= nil,
        'avro_schema must not be nil')

    if avro_helpers.avro_type(avro_schema) == 'record' then
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

            local gql_field_type = scalar_types.convert(field.type,
                {raise = true})

            fields[field.name] = {
                name = field.name,
                kind = gql_field_type,
            }
        end

        local res = core_types.nonNull(core_types.inputObject({
            name = avro_schema.name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        }))

        return res
    else
        local res = scalar_types.convert(avro_schema, {raise = false})
        if res == nil then
            error('unrecognized avro-schema type: ' ..
                json.encode(avro_schema))
        end
        return res
    end
end

return arguments

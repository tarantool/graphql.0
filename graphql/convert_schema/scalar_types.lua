--- Convert scalar avro-schema types to GraphQL types.

local json = require('json')
local core_types = require('graphql.core.types')
local avro_helpers = require('graphql.avro_helpers')

local utils = require('graphql.utils')
local check = utils.check

local scalar_types = {}

function scalar_types.convert(avro_schema, opts)
    local opts = opts or {}
    check(opts, 'opts', 'table')
    local raise = opts.raise or false
    check(raise, 'raise', 'boolean')

    local scalar_types = {
        ['int'] = core_types.int.nonNull,
        ['int*'] = core_types.int,
        ['long'] = core_types.long.nonNull,
        ['long*'] = core_types.long,
        ['float'] = core_types.float.nonNull,
        ['float*'] = core_types.float,
        ['double'] = core_types.double.nonNull,
        ['double*'] = core_types.double,
        ['boolean'] = core_types.boolean.nonNull,
        ['boolean*'] = core_types.boolean,
        ['string'] = core_types.string.nonNull,
        ['string*'] = core_types.string,
    }

    local avro_t = avro_helpers.avro_type(avro_schema)
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

return scalar_types

--- Collection of helpers to work with GraphQL types.

local utils = require('graphql.utils')
local check = utils.check

local core_types_helpers = {}

function core_types_helpers.nullable(gql_class)
    check(gql_class, 'gql_class', 'table')

    if gql_class.__type ~= 'NonNull' then return gql_class end

    assert(gql_class.ofType ~= nil, 'gql_class.ofType must not be nil')
    return core_types_helpers.nullable(gql_class.ofType)
end

function core_types_helpers.raw_gql_type(gql_class)
    check(gql_class, 'gql_class', 'table')

    if gql_class.ofType == nil then return gql_class end

    assert(gql_class.ofType ~= nil, 'gql_class.ofType must not be nil')
    return core_types_helpers.raw_gql_type(gql_class.ofType)
end

return core_types_helpers

--- Abstraction layer between a data collections (e.g. tarantool's spaces) and
--- the GraphQL query language.
---
--- Random notes:
---
--- * GraphQL top level statement must be a collection name. Arguments for this
---   statement match non-deducible field names of corresponding object and
---   passed to an accessor function in the filter argument.
---
--- Border cases:
---
--- * Unions: as GraphQL specification says "...no fields may be queried on
---   Union type without the use of typed fragments." Tarantool_graphql
---   behaves this way. So 'common fields' are not supported. This does NOT
---   work:
---
---     hero {
---         hero_id -- common field; does NOT work
---         ... on human {
---             name
---         }
---         ... on droid {
---             model
---         }
---     }
---
---
---
--- (GraphQL spec: http://facebook.github.io/graphql/October2016/#sec-Unions)
--- Also, no arguments are currently allowed for fragments.
--- See issue about this (https://github.com/facebook/graphql/issues/204)

local accessor_general = require('graphql.accessor_general')
local accessor_space = require('graphql.accessor_space')
local accessor_shard = require('graphql.accessor_shard')
local impl = require('graphql.impl')

local graphql = {}

-- constants
graphql.TIMEOUT_INFINITY = accessor_general.TIMEOUT_INFINITY

-- for backward compatibility
graphql.accessor_general = accessor_general
graphql.accessor_space = accessor_space
graphql.accessor_shard = accessor_shard

-- functions
graphql.new = impl.new
graphql.compile = impl.compile
graphql.execute = impl.execute
graphql.start_server = impl.start_server
graphql.stop_server = impl.stop_server

return graphql

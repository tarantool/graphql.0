local accessor_space = require('graphql.accessor_space')
local tarantool_graphql = require('graphql.tarantool_graphql')

local graphql = {}

graphql.accessor_space = accessor_space
graphql.new = tarantool_graphql.new

return graphql

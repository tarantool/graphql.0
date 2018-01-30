local accessor_general = require('graphql.accessor_general')
local accessor_space = require('graphql.accessor_space')
local accessor_shard = require('graphql.accessor_shard')
local tarantool_graphql = require('graphql.tarantool_graphql')

local graphql = {}

graphql.accessor_general = accessor_general
graphql.accessor_space = accessor_space
graphql.accessor_shard = accessor_shard
graphql.new = tarantool_graphql.new

return graphql

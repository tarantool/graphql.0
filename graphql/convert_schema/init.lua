--- Convert an extended avro-schema (collections) to a GraphQL schema.

local schema = require('graphql.convert_schema.schema')

local convert_schema = {}

convert_schema.convert = schema.convert

return convert_schema

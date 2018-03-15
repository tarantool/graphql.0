#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local graphql = require('graphql')
local testdata = require('test.testdata.common_testdata')
local utils = require('graphql.utils')
local yaml = require('yaml')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
testdata.init_spaces()

-- upload test data
testdata.fill_test_data()

-- acquire metadata
local metadata = testdata.get_test_metadata()
local schemas = metadata.schemas
local collections = metadata.collections
local service_fields = metadata.service_fields
local indexes = metadata.indexes

-- build accessor and graphql schemas
-- ----------------------------------

local accessor = graphql.accessor_space.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
})

local gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
})

-- run queries
-- -----------

local query_1 = [[
        query user_by_order($first_name: String, $description: String, $include: Boolean) {
            order_collection(description: $description) {
                order_id
                description
                user_connection @include(if: $include, first_name: $first_name) {
                    user_id
                    last_name
                    first_name
                }
            }
        }
    ]]

local gql_query_1 = gql_wrapper:compile(query_1)

-- should match 1 user
utils.show_trace(function()
    local variables_1_1 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        include = true
    }
    local result = gql_query_1:execute(variables_1_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

utils.show_trace(function()
    local variables_1_2 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        include = false
    }
    local result = gql_query_1:execute(variables_1_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

local query_2 = [[
        query user_by_order($first_name: String, $description: String, $skip: Boolean) {
            order_collection(description: $description) {
                order_id
                description
                user_connection @skip(if: $skip, first_name: $first_name) {
                    user_id
                    last_name
                    first_name
                }
            }
        }
    ]]

local gql_query_2 = gql_wrapper:compile(query_2)

utils.show_trace(function()
    local variables_2_1 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        skip = true
    }
    local result = gql_query_2:execute(variables_2_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

utils.show_trace(function()
    local variables_2_2 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        skip = false
    }
    local result = gql_query_2:execute(variables_2_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)
-- clean up
-- --------

testdata.drop_spaces()

os.exit()

#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')
local testdata = require('test.local.space_compound_index_testdata')

-- utils
-- -----

-- return an error w/o file name and line number
local function strip_error(err)
    return tostring(err):gsub('^.-:.-: (.*)$', '%1')
end

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

-- get a top-level object by a full compound primary key
-- -----------------------------------------------------

local query_1 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
        }
    }
]]

local gql_query_1 = gql_wrapper:compile(query_1)

utils.show_trace(function()
    local variables_1_1 = {user_str = 'user_str_b', user_num = 12}
    local result = gql_query_1:execute(variables_1_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- select top-level objects by a partial compound primary key (or maybe use
-- fullscan)
-- ------------------------------------------------------------------------

utils.show_trace(function()
    local variables_1_2 = {user_num = 12}
    local result = gql_query_1:execute(variables_1_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- select objects by a connection by a full compound index
-- -------------------------------------------------------

local query_2 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
            order_connection {
                order_str
                order_num
                description
            }
        }
    }
]]

utils.show_trace(function()
    local gql_query_2 = gql_wrapper:compile(query_2)
    local variables_2 = {user_str = 'user_str_b', user_num = 12}
    local result = gql_query_2:execute(variables_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- select object by a connection by a partial compound index
-- ---------------------------------------------------------

local query_3 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
            order_str_connection {
                order_str
                order_num
                description
            }
        }
    }
]]

utils.show_trace(function()
    local gql_query_3 = gql_wrapper:compile(query_3)
    local variables_3 = {user_str = 'user_str_b', user_num = 12}
    local result = gql_query_3:execute(variables_3)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- offset on top-level by a full compound primary key
-- --------------------------------------------------

local query_4 = [[
    query users($limit: Int, $offset: user_collection_offset) {
        user_collection(limit: $limit, offset: $offset) {
            user_str
            user_num
            last_name
            first_name
        }
    }
]]

local gql_query_4 = gql_wrapper:compile(query_4)

utils.show_trace(function()
    local variables_4_1 = {
        limit = 10,
        offset = {
            user_str = 'user_str_b',
            user_num = 12,
        }
    }
    local result = gql_query_4:execute(variables_4_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- offset on top-level by a partial compound primary key (expected to fail)
-- ------------------------------------------------------------------------

local ok, err = pcall(function()
    local variables_4_2 = {
        limit = 10,
        offset = {
            user_str = 'user_str_b',
        }
    }
    local result = gql_query_4:execute(variables_4_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

print(('RESULT: ok: %s; err: %s'):format(tostring(ok), strip_error(err)))

-- offset when using a connection by a full compound primary key
-- -------------------------------------------------------------

local query_5 = [[
    query users($user_str: String, $user_num: Long,
            $limit: Int, $offset: order_collection_offset) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
            order_connection(limit: $limit, offset: $offset) {
                order_str
                order_num
                description
            }
        }
    }
]]

local gql_query_5 = gql_wrapper:compile(query_5)

utils.show_trace(function()
    local variables_5_1 = {
        user_str = 'user_str_b',
        user_num = 12,
        limit = 4,
        offset = {
            order_str = 'order_str_b_2',
            order_num = 1202,
        }
    }
    local result = gql_query_5:execute(variables_5_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- offset when using a connection by a partial compound primary key (expected
-- to fail)
-- --------------------------------------------------------------------------

local ok, err = pcall(function()
    local variables_5_2 = {
        user_str = 'user_str_b',
        user_num = 12,
        limit = 4,
        offset = {
            order_str = 'order_str_b_2',
        }
    }
    local result = gql_query_5:execute(variables_5_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

print(('RESULT: ok: %s; err: %s'):format(tostring(ok), strip_error(err)))

-- clean up
-- --------

testdata.drop()

os.exit()

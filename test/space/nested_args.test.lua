#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')
local common_testdata = require('test.testdata.common_testdata')
local emails_testdata = require('test.testdata.nullable_1_1_conn_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
common_testdata.init_spaces()
emails_testdata.init_spaces()

-- upload test data
common_testdata.fill_test_data()
emails_testdata.fill_test_data()

local LOCALPART_FN = 1
local DOMAIN_FN = 2
local BODY_FN = 7

for _, tuple in box.space.email:pairs() do
    local body = tuple[BODY_FN]
    if body:match('^[xy]$') then
        local key = {tuple[LOCALPART_FN], tuple[DOMAIN_FN]}
        box.space.email:delete(key)
    end
end

-- acquire metadata
local common_metadata = common_testdata.get_test_metadata()
local emails_metadata = emails_testdata.get_test_metadata()

-- build accessor and graphql schemas
-- ----------------------------------

local common_accessor = graphql.accessor_space.new({
    schemas = common_metadata.schemas,
    collections = common_metadata.collections,
    service_fields = common_metadata.service_fields,
    indexes = common_metadata.indexes,
})

local common_gql_wrapper = graphql.new({
    schemas = common_metadata.schemas,
    collections = common_metadata.collections,
    accessor = common_accessor,
})

local emails_accessor = graphql.accessor_space.new({
    schemas = emails_metadata.schemas,
    collections = emails_metadata.collections,
    service_fields = emails_metadata.service_fields,
    indexes = emails_metadata.indexes,
})

local emails_gql_wrapper = graphql.new({
    schemas = emails_metadata.schemas,
    collections = emails_metadata.collections,
    accessor = emails_accessor,
})

-- run queries
-- -----------

local function print_and_return(...)
    print(...)
    return table.concat({...}, ' ') .. '\n'
end

local function format_result(name, query, variables, result)
    return ('RUN %s {{{\nQUERY\n%s\nVARIABLES\n%s\nRESULT\n%s\n}}}\n'):format(
        name, query:rstrip(), yaml.encode(variables), yaml.encode(result))
end

local function run_common_queries(gql_wrapper)
    local results = ''

    local query_1 = [[
        query user_by_order($user_id: String) {
            order_collection(user_connection: {user_id: $user_id}) {
                order_id
                description
                user_connection {
                    user_id
                    last_name
                    first_name
                }
            }
        }
    ]]

    utils.show_trace(function()
        local variables_1 = {user_id = 'user_id_1'}
        local gql_query_1 = gql_wrapper:compile(query_1)
        local result = gql_query_1:execute(variables_1)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    return results
end

local function run_emails_queries(gql_wrapper)
    local results = ''

    -- upside traversal (1:1 connections)
    -- ----------------------------------

    local query_upside = [[
        query emails_trace_upside($upside_body: String) {
            email(in_reply_to: {in_reply_to: {body: $upside_body}}) {
                body
                in_reply_to {
                    body
                    in_reply_to {
                        body
                    }
                }
            }
        }
    ]]

    utils.show_trace(function()
        local variables_upside = {upside_body = 'a'}
        local gql_query_upside = gql_wrapper:compile(query_upside)
        local result = gql_query_upside:execute(variables_upside)
        results = results .. print_and_return(format_result(
            'upside', query_upside, variables_upside, result))
    end)

    return results
end

run_common_queries(common_gql_wrapper)
run_emails_queries(emails_gql_wrapper)

-- clean up
-- --------

common_testdata.drop_spaces()
emails_testdata.drop_spaces()

os.exit()

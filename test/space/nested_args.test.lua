#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local tap = require('tap')
local yaml = require('yaml')
local graphql = require('graphql')
local test_utils = require('test.test_utils')
local common_testdata = require('test.testdata.common_testdata')
local emails_testdata = require('test.testdata.nullable_1_1_conn_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
common_testdata.init_spaces()
emails_testdata.init_spaces()

-- upload test data
local common_meta = common_testdata.meta or common_testdata.get_test_metadata()
local emails_meta = emails_testdata.meta or emails_testdata.get_test_metadata()
common_testdata.fill_test_data(box.space, common_meta)
emails_testdata.fill_test_data(box.space, emails_meta)

local avro_version = test_utils.major_avro_schema_version()

local LOCALPART_FN = 1
local DOMAIN_FN = 2
local BODY_FN = avro_version == 3 and 5 or 7

for _, tuple in box.space.email:pairs() do
    local body = tuple[BODY_FN]
    if body:match('^[xyz]$') then
        local key = {tuple[LOCALPART_FN], tuple[DOMAIN_FN]}
        box.space.email:delete(key)
    end
end

-- acquire metadata
local common_metadata = common_testdata.get_test_metadata()
local emails_metadata = emails_testdata.get_test_metadata()

-- build accessor and graphql schemas
-- ----------------------------------

local common_gql_wrapper = graphql.new({
    schemas = common_metadata.schemas,
    collections = common_metadata.collections,
    service_fields = common_metadata.service_fields,
    indexes = common_metadata.indexes,
    accessor = 'space',
    -- gh-137: timeout exceeded
    timeout_ms = 10000, -- 10 seconds
})

local emails_gql_wrapper = graphql.new({
    schemas = emails_metadata.schemas,
    collections = emails_metadata.collections,
    service_fields = emails_metadata.service_fields,
    indexes = emails_metadata.indexes,
    accessor = 'space',
    -- gh-137: timeout exceeded
    timeout_ms = 10000, -- 10 seconds
})

-- run queries
-- -----------

local test = tap.test('nested_args')
test:plan(2)

local function run_common_queries(gql_wrapper)
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

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local variables_1 = {user_id = 'user_id_1'}
    local result_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1)
    end)
    local exp_result_1 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          user_connection:
            user_id: user_id_1
            last_name: Ivanov
            first_name: Ivan
        - order_id: order_id_2
          description: second order of Ivan
          user_connection:
            user_id: user_id_1
            last_name: Ivanov
            first_name: Ivan
    ]]):strip())
    test:is_deeply(result_1.data, exp_result_1, '1')
end

local function run_emails_queries(gql_wrapper)
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

    local gql_query_upside = test_utils.show_trace(function()
        return gql_wrapper:compile(query_upside)
    end)

    local variables_upside = {upside_body = 'a'}
    local result_upside = test_utils.show_trace(function()
        return gql_query_upside:execute(variables_upside)
    end)
    local exp_result_upside = yaml.decode(([[
        ---
        email:
        - body: g
          in_reply_to:
            body: d
            in_reply_to:
              body: a
        - body: f
          in_reply_to:
            body: d
            in_reply_to:
              body: a
        - body: e
          in_reply_to:
            body: b
            in_reply_to:
              body: a
    ]]):strip())
    test:is_deeply(result_upside.data, exp_result_upside, 'upside')
end

run_common_queries(common_gql_wrapper)
run_emails_queries(emails_gql_wrapper)

assert(test:check(), 'check plan')

-- clean up
-- --------

common_testdata.drop_spaces()
emails_testdata.drop_spaces()

os.exit()

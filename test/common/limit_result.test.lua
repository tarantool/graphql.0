#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.user_order_item_testdata')
local graphql = require('graphql')

local e = graphql.error_codes

local function run_queries(gql_wrapper)
    local test = tap.test('result cnt')
    test:plan(2)

    local query = [[
        query object_result_max($user_id: Int, $description: String) {
            user_collection(id: $user_id) {
                id
                last_name
                first_name
                order_connection(description: $description){
                    id
                    user_id
                    description
                }
            }
        }
    ]]

    local gql_query = gql_wrapper:compile(query)
    local variables = {
        user_id = 5,
    }
    local result = gql_query:execute(variables)
    assert(result.data == nil, "this test should fail")
    assert(result.errors ~= nil, "this test should fail")
    local exp_err = 'resulting objects count (4) exceeds ' ..
        'resulting_object_cnt_max limit (3)'
    local err = result.errors[1].message
    local code = result.errors[1].extensions.error_code
    test:is_deeply({err, code}, {exp_err, e.RESULTING_OBJECTS_LIMIT_EXCEEDED},
        'resulting_object_cnt_max test')

    variables = {
        user_id = 5,
        description = "no such description"
    }
    local result = gql_query:execute(variables)
    assert(result.data == nil, "this test should fail")
    assert(result.errors ~= nil, "this test should fail")
    local exp_err = 'fetched objects count (6) exceeds ' ..
        'fetched_object_cnt_max limit (5)'
    local err = result.errors[1].message
    local code = result.errors[1].extensions.error_code
    test:is_deeply({err, code}, {exp_err, e.FETCHED_OBJECTS_LIMIT_EXCEEDED},
        'fetched_object_cnt_max test')

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
    graphql_opts = {
        resulting_object_cnt_max = 3,
        fetched_object_cnt_max = 5,
    }
})

os.exit()

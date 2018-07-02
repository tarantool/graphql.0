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
    test:plan(1)

    local query = [[
        query object_result_max {
            user_collection {
                id
                last_name
                first_name
                order_connection {
                    id
                    user_id
                    description
                }
            }
        }
    ]]

    local gql_query = gql_wrapper:compile(query)
    local variables = {}
    local result = gql_query:execute(variables)
    assert(result.data == nil, "this test should fail")
    assert(result.errors ~= nil, "this test should fail")
    local exp_err = 'query execution timeout exceeded timeout_ms limit (0.001 ms)'
    local err = result.errors[1].message
    local code = result.errors[1].extensions.error_code
    test:is_deeply({err, code}, {exp_err, e.TIMEOUT_EXCEEDED}, 'timeout test')

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
    graphql_opts = {
        timeout_ms = 0.001,
    }
})

os.exit()

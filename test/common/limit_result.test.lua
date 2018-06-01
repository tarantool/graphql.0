#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.user_order_item_testdata')

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
    local ok, result = pcall(gql_query.execute, gql_query, variables)
    assert(ok == false, "this test should fail")
    test:like(result,
              'count%[4%] exceeds limit%[3%] %(`resulting_object_cnt_max`',
              'resulting_object_cnt_max test')
    variables = {
        user_id = 5,
        description = "no such description"
    }
    ok, result = pcall(gql_query.execute, gql_query, variables)
    assert(ok == false, "this test should fail")
    test:like(result,
              'count%[6%] exceeds limit%[5%] %(`fetched_object_cnt_max`',
              'resulting_object_cnt_max test')

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

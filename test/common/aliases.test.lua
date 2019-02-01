#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')

local function run_queries(gql_wrapper)
    local test = tap.test('aliases')
    test:plan(2)

    local exp_result_1 = yaml.decode(([[
        ---
        u:
        - id: user_id_1
          o:
          - id: order_id_1
            m:
              s:
                a:
                  z: zip 1
          - id: order_id_2
            m:
              s:
                a:
                  z: zip 2
    ]]):strip())

    local query_1 = [[
        {
            u: user_collection(user_id: "user_id_1") {
                id: user_id
                o: order_connection {
                    id: order_id
                    m: order_metainfo_connection {
                        s: store {
                            a: address {
                                z: zip
                            }
                        }
                    }
                }
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local result_1 = gql_query_1:execute()
    test:is_deeply(result_1.data, exp_result_1, 'query with aliases')

    -- use cache-only requests in BFS executor

    local exp_result_2 = yaml.decode(([[
        ---
        o:
          - id: order_id_1
            u:
              id: user_id_1
    ]]):strip())

    local query_2 = [[
        {
            o: order_collection(
                order_id: "order_id_1"
                user_connection: {user_id: "user_id_1"}
            ) {
                id: order_id
                u: user_connection {
                    id: user_id
                }
            }
        }
    ]]

    local gql_query_2 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    local result_2 = gql_query_2:execute()
    test:is_deeply(result_2.data, exp_result_2,
        'query with aliases and cache-only requests')

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {run_queries = run_queries})

os.exit()

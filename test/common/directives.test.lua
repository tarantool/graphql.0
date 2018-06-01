#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.utils')
local testdata = require('test.testdata.common_testdata')

local function run_queries(gql_wrapper)
    local test = tap.test('directives')
    test:plan(4)

    local query_1 = [[
            query user_by_order($first_name: String, $description: String,
                    $include: Boolean) {
                order_collection(description: $description) {
                    order_id
                    description
                    user_connection @include(if: $include,
                            first_name: $first_name) {
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

    -- {{{ 1_1

    local variables_1_1 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        include = true
    }

    -- should match 1 user
    local result_1_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)

    local exp_result_1_1 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          user_connection:
            user_id: user_id_1
            last_name: Ivanov
            first_name: Ivan
    ]]):strip())

    test:is_deeply(result_1_1, exp_result_1_1, '1_1')

    -- }}}
    -- {{{ 1_2

    local variables_1_2 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        include = false
    }

    local result_1_2 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_2)
    end)

    local exp_result_1_2 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
    ]]):strip())

    test:is_deeply(result_1_2, exp_result_1_2, '1_2')

    -- }}}

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

    local gql_query_2 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    -- {{{ 2_1

    local variables_2_1 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        skip = true
    }

    local result_2_1 = test_utils.show_trace(function()
        return gql_query_2:execute(variables_2_1)
    end)

    local exp_result_2_1 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
    ]]):strip())

    test:is_deeply(result_2_1, exp_result_2_1, '2_1')

    -- }}}
    -- {{{ 2_2

    local variables_2_2 = {
        first_name = 'Ivan',
        description = 'first order of Ivan',
        skip = false
    }

    local result_2_2 = test_utils.show_trace(function()
        return gql_query_2:execute(variables_2_2)
    end)

    local exp_result_2_2 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          user_connection:
            user_id: user_id_1
            last_name: Ivanov
            first_name: Ivan
    ]]):strip())

    test:is_deeply(result_2_2, exp_result_2_2, '2_2')

    -- }}}

    assert(test:check(), 'check plan')
end


box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

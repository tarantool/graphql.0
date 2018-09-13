#!/usr/bin/env tarantool

-- https://github.com/tarantool/graphql/issues/13

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')

local function run_queries(gql_wrapper)
    local test = tap.test('expressions')
    test:plan(6)

    local exp_result_1_and_2 = test_utils.deeply_number_tostring(yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_4
          price: 4.3333333333333
        - order_id: order_id_5
          price: 4.6666666666667
    ]]):strip()))

    -- {{{ test expresion (use variables and &&)

    -- immediate filter value
    local query_1 = [[
        query get_by_expr($price_from: Double, $price_to: Double) {
            order_collection(
                filter: "price >= $price_from && price < $price_to"
            ) {
                order_id
                price
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local variables_1 = {
        price_from = 4,
        price_to = 5,
    }

    local result_1 = gql_query_1:execute(variables_1)
    local result_1_data = test_utils.deeply_number_tostring(result_1.data)
    test:is_deeply(result_1_data, exp_result_1_and_2,
        'expressions with variables and && (immediate filter value)')

    -- variable as a filter value
    local query_1v = [[
        query get_by_expr(
            $filter: String
            $price_from: Double
            $price_to: Double
        ) {
            order_collection(filter: $filter) {
                order_id
                price
            }
        }
    ]]

    local gql_query_1v = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1v)
    end)

    local variables_1v = {
        filter = "price >= $price_from && price < $price_to",
        price_from = 4,
        price_to = 5,
    }

    local result_1v = gql_query_1v:execute(variables_1v)
    local result_1v_data = test_utils.deeply_number_tostring(result_1v.data)
    test:is_deeply(result_1v_data, exp_result_1_and_2,
        'expressions with variables and && (variable as a filter value)')

    -- }}}

    -- {{{ test expresion (use immediate values and &&)

    -- immediate filter value
    local query_2 = [[
        query get_by_expr {
            order_collection(
                filter: "price >= 4 && price < 5"
            ) {
                order_id
                price
            }
        }
    ]]

    local gql_query_2 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    local variables_2 = {}

    local result_2 = test_utils.show_trace(function()
        return gql_query_2:execute(variables_2)
    end)

    local result_2_data = test_utils.deeply_number_tostring(result_2.data)
    test:is_deeply(result_2_data, exp_result_1_and_2,
        'expression with immediate values and && (immediate filter value)')

    -- variable as a filter value
    local query_2v = [[
        query get_by_expr($filter: String) {
            order_collection(filter: $filter) {
                order_id
                price
            }
        }
    ]]

    local gql_query_2v = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2v)
    end)

    local variables_2v = {
        filter = "price >= 4 && price < 5",
    }

    local result_2v = gql_query_2v:execute(variables_2v)
    local result_2v_data = test_utils.deeply_number_tostring(result_2v.data)
    test:is_deeply(result_2v_data, exp_result_1_and_2,
        'expression with immediate values and && (variable as a filter value)')

    -- }}}

    -- {{{ test expression with nested fields and ||

    local exp_result_3 = test_utils.deeply_number_tostring(yaml.decode(([[
        ---
        order_metainfo_collection:
        - order_metainfo_id: order_metainfo_id_1
          store:
            address:
              city: city 1
        - order_metainfo_id: order_metainfo_id_42
          store:
            address:
              city: city 42
    ]]):strip()))

    -- immediate filter value
    local query_3 = [[
        query get_by_expr {
            order_metainfo_collection(
                filter: "store.address.city == \"city 1\" || store.address.city == \"city 42\""
            ) {
                order_metainfo_id
                store {
                    address {
                        city
                    }
                }
            }
        }
    ]]

    local gql_query_3 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_3)
    end)

    local variables_3 = {}

    local result_3 = gql_query_3:execute(variables_3)
    local result_3_data = test_utils.deeply_number_tostring(result_3.data)
    test:is_deeply(result_3_data, exp_result_3,
        'expression with nested fields and || (immediate filter value)')

    -- variable as a filter value
    local query_3v = [[
        query get_by_expr($filter: String) {
            order_metainfo_collection(filter: $filter) {
                order_metainfo_id
                store {
                    address {
                        city
                    }
                }
            }
        }
    ]]

    local gql_query_3v = test_utils.show_trace(function()
        return gql_wrapper:compile(query_3v)
    end)

    local variables_3v = {
        filter = [[
            store.address.city == "city 1" ||
            store.address.city == "city 42"
        ]],
    }

    local result_3v = gql_query_3v:execute(variables_3v)
    local result_3v_data = test_utils.deeply_number_tostring(result_3v.data)
    test:is_deeply(result_3v_data, exp_result_3,
        'expression with nested fields and || (variable as a filter value)')

    -- }}}

    -- XXX: use an undefined variable inside a value of a filter variable

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
    graphql_opts = {
        -- gh-137: timeout exceeded
        timeout_ms = 10000, -- 10 seconds
    }
})

os.exit()

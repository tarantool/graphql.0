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
    local test = tap.test('pcre')
    test:plan(4)

    local query_1 = [[
        query users($offset: String, $first_name_re: String,
                $middle_name_re: String) {
            user_collection(pcre: {first_name: $first_name_re,
                    middle_name: $middle_name_re}, offset: $offset) {
                first_name
                middle_name
                last_name
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    -- {{{ regexp match

    local variables_1_1 = {
        first_name_re = '(?i)^i',
        middle_name_re = 'ich$',
    }

    local result_1_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)

    local exp_result_1_1 = yaml.decode(([[
        ---
        user_collection:
        - last_name: Ivanov
          first_name: Ivan
          middle_name: Ivanovich
    ]]):strip())

    test:is_deeply(result_1_1, exp_result_1_1, '1_1')

    -- }}}
    -- {{{ offset + regexp match

    local variables_1_2 = {
        user_id = 'user_id_1',
        first_name_re = '^V',
    }

    local result_1_2 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_2)
    end)

    local exp_result_1_2 = yaml.decode(([[
        ---
        user_collection:
        - last_name: Pupkin
          first_name: Vasiliy
    ]]):strip())

    test:is_deeply(result_1_2, exp_result_1_2, '1_2')

    -- }}}
    -- {{{ UTF-8 in regexp

    local variables_1_3 = {
        first_name_re = '(?i)^и',
        middle_name_re = 'ич$',
    }

    local result_1_3 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_3)
    end)

    local exp_result_1_3 = yaml.decode(([[
        ---
        user_collection:
        - last_name: Иванов
          first_name: Иван
          middle_name: Иванович
    ]]):strip())

    test:is_deeply(result_1_3, exp_result_1_3, '1_3')

    -- }}}

    -- {{{ regexp match with immediate arguments

    local query_1i = [[
        query users {
            user_collection(pcre: {
                first_name: "(?i)^i",
                middle_name: "ich$",
            }) {
                first_name
                middle_name
                last_name
            }
        }
    ]]

    local gql_query_1i = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1i)
    end)

    local result_1i_1 = test_utils.show_trace(function()
        return gql_query_1i:execute({})
    end)

    test:is_deeply(result_1i_1, exp_result_1_1, '1i_1')

    -- }}}

    assert(test:check(), 'check plan')
end


box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

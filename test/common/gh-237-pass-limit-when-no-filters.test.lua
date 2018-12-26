#!/usr/bin/env tarantool

-- https://github.com/tarantool/graphql/issues/237

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')
local storage = require('graphql.storage')

local function run_queries(gql_wrapper)
    local test = tap.test('pass limit to storages when no filters present')
    local conf_name = test_utils.get_conf_name()
    local replicasets_count = test_utils.get_replicasets_count()
    local block_size = storage.get_block_size()
    -- 3924 is amount of tuples in order_collection; keep it is sync with
    -- test/testdata/common_testdata.lua::fill_test_data() code
    local no_limit_size = math.min(replicasets_count * block_size, 3924)
    test:plan(16)

    local exp_result_1_1_to_1_5 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
    ]]):strip())

    local exp_result_1_6_to_1_7 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_10
    ]]):strip())

    local query_1 = [[
        query first_order(
            $description: String
            $description_re: String
            $user_id: String
            $offset: String
        ) {
            order_collection(
                limit: 1
                description: $description
                user_id: $user_id
                pcre: {description: $description_re}
                offset: $offset
            ) {
                order_id
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    -- Note: all case descriptions below are actual only for uppermost
    -- collection filtering.

    -- case: when only limit argument is passed for an uppermost collection we
    -- can pass it to storages

    local variables_1_1 = {}

    local result_1_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)

    test:is_deeply(result_1_1.data, exp_result_1_1_to_1_5,
        'only limit case: check data')
    test:ok(result_1_1.meta.statistics.fetched_object_cnt <= replicasets_count,
        'only limit case: check meta')

    -- case: object argument w/o an index underhood should discard limit
    -- passthrough

    local variables_1_2 = {
        description = 'first order of Ivan',
    }

    local result_1_2 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_2)
    end)

    test:is_deeply(result_1_2.data, exp_result_1_1_to_1_5,
        'limit + non-index object argument case: check data')
    test:ok(conf_name == 'space' or
        result_1_2.meta.statistics.fetched_object_cnt >= no_limit_size,
        'limit + non-index object argument case: check meta')

    -- case: when one object argument with an index underhood is passed we can
    -- pass a limit to storages

    local variables_1_3 = {
        user_id = 'user_id_1',
    }

    local result_1_3 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_3)
    end)

    test:is_deeply(result_1_3.data, exp_result_1_1_to_1_5,
        'limit + index object argument case: check data')
    test:ok(result_1_3.meta.statistics.fetched_object_cnt <= replicasets_count,
        'limit + index object argument case: check meta')

    -- case: object argument that is not choosen for index lookup (say, because
    -- other one is choosen) should discard limit passthrough

    -- note: lookup by user_id_index of order_collection with 'user_id_1' gives
    -- two results

    local variables_1_4 = {
        user_id = 'user_id_1',
        description = 'first order of Ivan',
    }

    local result_1_4 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_4)
    end)

    test:is_deeply(result_1_4.data, exp_result_1_1_to_1_5,
        'limit + index and non-index object arguments case: check data')
    test:ok(conf_name == 'space' or
        result_1_4.meta.statistics.fetched_object_cnt > 1,
        'limit + index and non-index object arguments case: check meta')

    -- case: list argument (except 'limit', empty 'pcre' and 'offset') should
    -- discard limit passthrough

    local variables_1_5 = {
        description_re = '^first order of Ivan$',
    }

    local result_1_5 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_5)
    end)

    test:is_deeply(result_1_5.data, exp_result_1_1_to_1_5,
        'limit + list argument case: check data')
    test:ok(conf_name == 'space' or
        result_1_5.meta.statistics.fetched_object_cnt >= no_limit_size,
        'limit + list argument case: check meta')

    -- case: when only 'offset' list argument is passed we can pass a limit to
    -- storages (because 'offset' uses an index)

    local variables_1_6 = {
        offset = 'order_id_1',
    }

    local result_1_6 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_6)
    end)

    test:is_deeply(result_1_6.data, exp_result_1_6_to_1_7,
        'limit + offset case: check data')
    test:ok(result_1_6.meta.statistics.fetched_object_cnt <= replicasets_count,
        'limit + offset case: check meta')

    -- case: offset and other list argument (say, 'pcre') should discard limit
    -- passthrought

    local variables_1_7 = {
        offset = 'order_id_1',
        description_re = '^order of user.*$',
    }

    local result_1_7 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_7)
    end)

    test:is_deeply(result_1_7.data, exp_result_1_6_to_1_7,
        'limit + offset + non-empty pcre case: check data')
    -- -1 is due to offset
    test:ok(conf_name == 'space' or
        result_1_7.meta.statistics.fetched_object_cnt >= no_limit_size - 1,
        'limit + offset + non-empty pcre case: check meta')

    -- case: when an extra argument passed we can pass a limit to storages
    -- (because extra args is mutation args and do nothing with filtering)

    local mutation_1 = [[
        mutation update_order(
            $description: String
        ) {
            order_collection(
                limit: 1
                update: {description: $description}
            ) {
                order_id
            }
        }
    ]]

    local gql_mutation_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(mutation_1)
    end)

    -- update to the same value, it does not matter
    local variables_m_1_1 = {
        description = 'first order of Ivan',
    }

    local result_m_1_1 = test_utils.show_trace(function()
        return gql_mutation_1:execute(variables_m_1_1)
    end)

    test:is_deeply(result_m_1_1.data, exp_result_1_1_to_1_5,
        'limit + extra argument case: check data')
    test:ok(result_m_1_1.meta.statistics.fetched_object_cnt <=
        replicasets_count, 'limit + extra argument case: check meta')

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

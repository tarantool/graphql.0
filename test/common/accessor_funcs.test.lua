#!/usr/bin/env tarantool

-- This test verifies that user-provided accessor functions are called and a
-- user_context option is correctly passed. It defines a set of queries and
-- mutations to touch all available functions (including cache_* ones when they
-- are available).

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')

-- XXX: It seems that our API is not good, because we need to wrap accessor
-- functions in that way. I think we shold pass `default` parameter to each
-- function (and also factor our default *flatten functions from
-- accessor_general).

-- func name -> opts position
local func_opts_pos = {
    {is_collection_exists = 3},
    {get_index = 4},
    {get_primary_index = 3},
    {unflatten_tuple = 4},
    {flatten_object = 4},
    {xflatten = 4},
    {insert_tuple = 4},
    {update_tuple = 5},
    {delete_tuple = 4},
    {cache_fetch = 3},
    -- {cache_delete = 3},
    {cache_truncate = 2},
    {cache_lookup = 6},
}

local function setup_funcs(gql_wrapper)
    local accessor = gql_wrapper.internal.cfg.accessor
    local funcs = accessor.funcs

    assert(accessor:cache_is_supported() == test_utils.is_cache_supported())

    for _, v in ipairs(func_opts_pos) do
        local name, opts_pos = next(v)
        local default = funcs[name]
        if default ~= nil then
            funcs[name] = function(...)
                local opts = select(opts_pos, ...)
                local user_context = opts.user_context
                user_context[name] = true
                return default(...)
            end
        end
    end
end

local function run_queries(gql_wrapper)
    local queries = {
        -- trigger is_collection_exists, get_index, get_primary_index,
        -- unflatten_tuple and all cache functions if any
        {
            'complex select',
            query = [[
                {
                    order_collection(
                        limit: 2
                        user_connection: {user_id: "user_id_1"}
                    ) {
                        order_id
                    }
                }
            ]],
            exp_result = yaml.decode(([[
                ---
                order_collection:
                - order_id: order_id_1
                - order_id: order_id_2
            ]]):strip()),
        },
        -- trigger flatten_object, insert_tuple (+some of listed above)
        {
            'insert',
            query = [[
                mutation {
                    user_collection(insert: {
                        user_id: "user_id_new_1"
                        first_name: "Peter"
                        last_name: "Petrov"
                    }) {
                        user_id
                    }
                }
            ]],
            exp_result = yaml.decode(([[
                ---
                user_collection:
                - user_id: user_id_new_1
            ]]):strip()),
        },
        -- trigger xflatten, update_tuple (+some of listed above)
        {
            'update',
            query = [[
                mutation {
                    user_collection(user_id: "user_id_new_1", update: {
                        first_name: "Vasiliy"
                    }) {
                        user_id
                    }
                }
            ]],
            exp_result = yaml.decode(([[
                ---
                user_collection:
                - user_id: user_id_new_1
            ]]):strip()),
        },
        -- trigger delete_tuple (+some of listed above)
        {
            'delete',
            query = [[
                mutation {
                    user_collection(user_id: "user_id_new_1", delete: true) {
                        user_id
                    }
                }
            ]],
            exp_result = yaml.decode(([[
                ---
                user_collection:
                - user_id: user_id_new_1
            ]]):strip()),
        },
    }

    local test = tap.test('accessor_funcs')
    test:plan(#queries + 12)

    setup_funcs(gql_wrapper)

    local user_context = {}

    for _, q in ipairs(queries) do
        test_utils.show_trace(function()
            local gql_query = gql_wrapper:compile(q.query)
            local result = gql_query:execute(nil, nil,
                {user_context = user_context})
            local description = ('result of query "%s"'):format(q[1])
            test:is_deeply(result.data, q.exp_result, description)
        end)
    end

    for _, v in ipairs(func_opts_pos) do
        local name = next(v)
        local description = ('verify function "%s" was called'):format(name)
        local skip = name:startswith('cache_') and
            (test_utils.get_executor_name() == 'dfs' or
            not test_utils.is_cache_supported())
        if skip then
            test:skip(description)
        else
            test:ok(user_context[name] ~= nil, description)
        end
    end

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
    graphql_opts = {
        timeout_ms = 10000, -- 10 seconds
    }
})

os.exit()

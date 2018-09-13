#!/usr/bin/env tarantool

-- requires
-- --------

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local bench = require('test.bench.bench')
local testdata = require('test.testdata.bench_testdata')

-- functions
-- ---------

local function bench_prepare(state, ctx)
    state.gql_wrapper = bench.bench_prepare_helper(testdata, ctx, state.virtbox)
    local query = [[
        query match_by_user_id($user_id: String) {
            user(user_id: $user_id) {
                user_id
                first_name
                middle_name
                last_name
            }
        }
    ]]
    state.variables = {
        user_id = 'user_id_42',
    }
    state.gql_query = state.gql_wrapper:compile(query)
end

local function bench_iter(state)
    return state.gql_query:execute(state.variables)
end

-- run
-- ---

box.cfg({})

bench.run('nesting-1-1', {
    init_function = testdata.init_spaces,
    cleanup_function = testdata.drop_spaces,
    bench_prepare = bench_prepare,
    meta = testdata.meta or testdata.get_test_metadata(),
    bench_iter = bench_iter,
    iterations = {
        space = 1000000,
        shard = 100000,
    },
    checksums = {
        space = 1941710138,
        shard = 1750365033,
    },
})

os.exit()

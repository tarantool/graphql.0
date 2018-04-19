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

local function bench_prepare(state)
    state.gql_wrapper = bench.bench_prepare_helper(testdata, state.shard)
    local query = [[
        query match_by_user_and_passport_id($user_id: String,
                $passport_id: String) {
            user(user_id: $user_id, user_to_passport_c: {
                    passport_id: $passport_id}) {
                user_id
                first_name
                middle_name
                last_name
                user_to_passport_c {
                    passport_id
                }
            }
        }
    ]]
    state.variables = {
        user_id = 'user_id_42',
        passport_id = 'passport_id_42',
    }
    state.gql_query = state.gql_wrapper:compile(query)
end

local function bench_iter(state)
    return state.gql_query:execute(state.variables)
end

-- run
-- ---

box.cfg({})

bench.run('nesting-2-1-1', {
    init_function = testdata.init_spaces,
    cleanup_function = testdata.drop_spaces,
    bench_prepare = bench_prepare,
    bench_iter = bench_iter,
    iterations = {
        space = 100000,
        shard = 10000,
    },
    checksums = {
        space = 1500062808,
        shard = 478898394,
    },
})

os.exit()

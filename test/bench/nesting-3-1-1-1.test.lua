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
    local meta = testdata.meta or testdata.get_test_metadata()
    state.gql_wrapper = bench.bench_prepare_helper(testdata, state.shard, meta)
    local query = [[
        query match_by_user_and_passport($user_id: String, $number: String) {
            user(user_id: $user_id, user_to_passport_c: {
                    passport_c: {number: $number}}) {
                user_id
                first_name
                middle_name
                last_name
                user_to_passport_c {
                    passport_c {
                        passport_id
                        number
                    }
                }
            }
        }
    ]]
    state.variables = {
        user_id = 'user_id_42',
        number = 'number_42',
    }
    state.gql_query = state.gql_wrapper:compile(query)
end

local function bench_iter(state)
    return state.gql_query:execute(state.variables)
end

-- run
-- ---

box.cfg({})

bench.run('nesting-3-1-1-1', {
    init_function = testdata.init_spaces,
    cleanup_function = testdata.drop_spaces,
    bench_prepare = bench_prepare,
    bench_iter = bench_iter,
    iterations = {
        space = 100000,
        shard = 10000,
    },
    checksums = {
        space = 839993960,
        shard = 922069577,
    },
})

os.exit()

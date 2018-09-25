#!/usr/bin/env tarantool

local tap = require('tap')
local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
local cur_dir = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', ''))
package.path =
    cur_dir .. '/../../?/init.lua' .. ';' ..
    cur_dir .. '/../../?.lua' .. ';' ..
    package.path

local find_index = require('graphql.find_index')
local testdata = require('test.testdata.common_testdata')

local cases = {
    {
        name = 'find one-part index',
        collection_name = 'user_collection',
        from = {
            collection_name = nil,
            connection_name = 'user_collection',
            destination_args_names = {},
            destination_args_values = {},
        },
        filter = {user_id = 'user_id_1'},
        args = {},
        exp_full_match = true,
        exp_index_name = 'user_id_index',
        exp_filter = {},
        exp_index_value = {'user_id_1'},
        exp_pivot = nil,
    },
}

local test = tap.test('find_index')
test:plan(#cases)

local db_schema = testdata.meta or testdata.get_test_metadata()
local index_finder = find_index.new(db_schema)

local function run_case(test, index_finder, case)
    local full_match, index_name, filter, index_value, pivot =
        index_finder:find(case.collection_name, case.from, case.filter,
        case.args)
    local res = {
        full_match = full_match,
        index_name = index_name,
        filter = filter,
        index_value = index_value,
        pivot = pivot,
    }
    local exp = {
        full_match = case.exp_full_match,
        index_name = case.exp_index_name,
        filter = case.exp_filter,
        index_value = case.exp_index_value,
        pivot = case.exp_pivot,
    }
    test:is_deeply(res, exp, case.name)
end

for _, case in ipairs(cases) do
    run_case(test, index_finder, case)
end

os.exit(test:check() == true and 0 or 1)

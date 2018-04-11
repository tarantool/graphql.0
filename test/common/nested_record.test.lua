#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local multirunner = require('test.common.lua.multirunner')
local testdata = require('test.common.lua.test_data_nested_record')
local graphql = require('graphql')
local utils = require('graphql.utils')
local test_run = utils.optional_require('test_run')
if test_run then
    test_run = test_run.new()
end

box.cfg({})

local function run(setup_name, shard)
    print(setup_name)

    local virtbox = shard or box.space

    local gql_wrapper = graphql.new({
        schemas = testdata.meta.schemas,
        collections = testdata.meta.collections,
        service_fields = testdata.meta.service_fields,
        indexes = testdata.meta.indexes,
        accessor = shard and 'shard' or 'space',
    })

    testdata.fill_test_data(virtbox)
    print(testdata.run_queries(gql_wrapper))
end

multirunner.run(test_run, testdata.init_spaces, testdata.drop_spaces, run)

os.exit()

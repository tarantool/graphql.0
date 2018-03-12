#!/usr/bin/env tarantool

local fio = require('fio')
local multirunner = require('multirunner')
local testdata = require('test_data_nested_record')
local test_run = require('test_run').new()

box.cfg({})

local function run(setup_name, shard, graphql)
    print(setup_name)

    local accessor_class = shard and graphql.accessor_shard or
        graphql.accessor_space
    local virtbox = shard or box.space

    local accessor = accessor_class.new({
        schemas = testdata.meta.schemas,
        collections = testdata.meta.collections,
        service_fields = testdata.meta.service_fields,
        indexes = testdata.meta.indexes,
    })

    local gql_wrapper = graphql.new({
        schemas = testdata.meta.schemas,
        collections = testdata.meta.collections,
        accessor = accessor,
    })

    testdata.fill_test_data(virtbox)
    print(testdata.run_queries(gql_wrapper))
end

multirunner.run(test_run, testdata.init_spaces, testdata.drop_spaces, run)

os.exit()

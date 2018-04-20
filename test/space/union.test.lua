#!/usr/bin/env tarantool

box.cfg { background = false }
local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local graphql = require('graphql')
local testdata = require('test.testdata.union_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------


-- init box and data schema
testdata.init_spaces()

-- upload test data
testdata.fill_test_data()

-- acquire metadata
local metadata = testdata.get_test_metadata()
local schemas = metadata.schemas
local collections = metadata.collections
local service_fields = metadata.service_fields
local indexes = metadata.indexes
local utils = require('graphql.utils')

-- build accessor and graphql schemas
-- ----------------------------------
local accessor = utils.show_trace(function()
    return graphql.accessor_space.new({
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    })
end)

local gql_wrapper = utils.show_trace(function()
    return graphql.new({
        schemas = schemas,
        collections = collections,
        accessor = accessor,
    })
end)

-- run queries
-- -----------

testdata.run_queries(gql_wrapper)

-- clean up
-- --------

testdata.drop_spaces()

os.exit()
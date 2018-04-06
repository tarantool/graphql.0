#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
package.path

local graphql = require('graphql')
local testdata = require('test.testdata.common_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- test with accessor_space

-- init box and data schema
box.cfg{background = false}
testdata.init_spaces()

-- upload test data
testdata.fill_test_data()

-- acquire metadata
local metadata = testdata.get_test_metadata()
local schemas = metadata.schemas
local collections = metadata.collections
local service_fields = metadata.service_fields
local indexes = metadata.indexes

-- build accessor and graphql schemas
-- ----------------------------------



local gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
    accessor = 'space'
})

-- run queries
-- -----------

testdata.run_queries(gql_wrapper)

-- clean up
-- --------

testdata.drop_spaces()


os.exit()

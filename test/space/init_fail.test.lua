#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local tap = require('tap')
local graphql = require('graphql')
local utils = require('graphql.utils')
local test_utils = require('test.test_utils')
local vb = require('test.virtual_box')
local testdata = require('test.testdata.compound_index_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
testdata.init_spaces()

-- acquire metadata
local metadata = testdata.get_test_metadata()

-- upload test data
local virtbox = vb.get_virtbox_for_accessor('space', {meta = metadata})
testdata.fill_test_data(virtbox)

-- inject an error into the metadata
-- ---------------------------------

local saved_part =
    metadata.collections.order_collection.connections[1].parts[2]
metadata.collections.order_collection.connections[1].parts[2] = nil

-- build accessor and graphql schemas
-- ----------------------------------

local function create_gql_wrapper(metadata)
    return graphql.new(utils.merge_tables({
        schemas = metadata.schemas,
        collections = metadata.collections,
        service_fields = metadata.service_fields,
        indexes = metadata.indexes,
        accessor = 'space',
    }, test_utils.test_conf_graphql_opts()))
end

local test = tap.test('init_fail')
test:plan(3)

local ok, err = pcall(create_gql_wrapper, metadata)
local exp_err = '1:1 connection "user_connection" of collection ' ..
    '"order_collection" has less fields than the index of ' ..
    '"user_str_num_index" collection (cannot prove uniqueness of the partial ' ..
    'index)'
test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
    'not enough fields')

-- restore back cut part
metadata.collections.order_collection.connections[1].parts[2] = saved_part

local ok, res = pcall(create_gql_wrapper, metadata)
test:is_deeply({ok, type(res)}, {true, 'table'}, 'enough fields')

-- multiple primary indexes
-- ------------------------

-- inject an error into the metadata
metadata.indexes.user_collection.user_str_index = {
    service_fields = {},
    fields = {'user_str'},
    index_type = 'tree',
    unique = true,
    primary = true,
}

local ok, err = pcall(create_gql_wrapper, metadata)
local exp_err = 'several indexes were marked as primary in the ' ..
    '"user_collection" collection, at least "user_str_num_index" and ' ..
    '"user_str_index"'
test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
    'multiple primary indexes')

-- restore metadata back
metadata.indexes.user_collection.user_str_index = nil

assert(test:check(), 'check plan')

-- clean up
-- --------

testdata.drop_spaces()

os.exit()

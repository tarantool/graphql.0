#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local graphql = require('graphql')
local testdata = require('test.testdata.compound_index_testdata')
local test_utils = require('test.utils')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
testdata.init_spaces()

-- upload test data
testdata.fill_test_data()

-- acquire metadata
local metadata = testdata.get_test_metadata()

-- inject an error into the metadata
-- ---------------------------------

local saved_part =
    metadata.collections.order_collection.connections[1].parts[2]
metadata.collections.order_collection.connections[1].parts[2] = nil

-- build accessor and graphql schemas
-- ----------------------------------

local function create_gql_wrapper(metadata)
    local accessor = graphql.accessor_space.new({
        schemas = metadata.schemas,
        collections = metadata.collections,
        service_fields = metadata.service_fields,
        indexes = metadata.indexes,
    })

    return graphql.new({
        schemas = metadata.schemas,
        collections = metadata.collections,
        accessor = accessor,
    })
end

local ok, err = pcall(create_gql_wrapper, metadata)
print(('INIT: ok: %s; err: %s'):format(tostring(ok),
    test_utils.strip_error(err)))

-- restore back cut part
metadata.collections.order_collection.connections[1].parts[2] = saved_part

local ok, res = pcall(create_gql_wrapper, metadata)
print(('INIT: ok: %s; type(res): %s'):format(tostring(ok), type(res)))

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
print(('INIT: ok: %s; err: %s'):format(tostring(ok),
    test_utils.strip_error(err)))

-- restore metadata back
metadata.indexes.user_collection.user_str_index = nil

-- clean up
-- --------

testdata.drop_spaces()

os.exit()

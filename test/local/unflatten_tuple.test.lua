#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local avro = require('avro_schema')
local graphql = require('graphql')
local testdata = require('test.testdata.common_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

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

local ok, handle = avro.create(schemas.user)
assert(ok, 'avro schema create: ' .. tostring(handle))
local ok, model = avro.compile({handle, service_fields = {'int'}})
assert(ok, 'avro schema compile: ' .. tostring(model))
local user_model = model

local ok, handle = avro.create(schemas.order)
assert(ok, 'avro schema create: ' .. tostring(handle))
local ok, model = avro.compile({handle})
assert(ok, 'avro schema compile: ' .. tostring(model))
local order_model = model

local function unflatten_tuple(collection_name, tuple, default)
    if collection_name == 'user_collection' then
        local ok, obj = user_model.unflatten(tuple)
        assert(ok, 'unflatten: ' .. tostring(obj))
        obj.first_name = obj.first_name .. '$'
        obj.last_name = obj.last_name .. '$'
        return obj
    elseif collection_name == 'order_collection' then
        local ok, obj = order_model.unflatten(tuple)
        assert(ok, 'unflatten: ' .. tostring(obj))
        obj.description = obj.description .. '$'
        return obj
    end
    error('unexpected collection_name: ' .. tostring(collection_name))
end

local accessor = graphql.accessor_space.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
}, {
    unflatten_tuple = unflatten_tuple,
})

local gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
})

-- run queries
-- -----------

testdata.run_queries(gql_wrapper)

-- clean up
-- --------

testdata.drop_spaces()

os.exit()

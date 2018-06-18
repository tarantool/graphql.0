#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local tap = require('tap')
local yaml = require('yaml')
local avro = require('avro_schema')
local graphql = require('graphql')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')

local utils = require('graphql.utils')
local check = utils.check

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
testdata.init_spaces()

-- acquire metadata
local metadata = testdata.get_test_metadata()
local schemas = metadata.schemas
local collections = metadata.collections
local service_fields = metadata.service_fields
local indexes = metadata.indexes

-- upload test data
testdata.fill_test_data(box.space, metadata)

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

local function unflatten_tuple(self, collection_name, tuple, default)
    check(self, 'self', 'table')
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

local function run_queries(gql_wrapper)
    local test = tap.test('unflatten_tuple')
    test:plan(1)

    local query_1 = [[
        query user_by_order($order_id: String) {
            order_collection(order_id: $order_id) {
                order_id
                description
                user_connection {
                    user_id
                    last_name
                    first_name
                }
            }
        }
    ]]

    local exp_result_1 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan$
          user_connection:
            user_id: user_id_1
            last_name: Ivanov$
            first_name: Ivan$
    ]]):strip())

    local result = test_utils.show_trace(function()
        local variables_1 = {order_id = 'order_id_1'}
        local gql_query_1 = gql_wrapper:compile(query_1)
        return gql_query_1:execute(variables_1)
    end)

    test:is_deeply(result, exp_result_1, '1')

    assert(test:check(), 'check plan')
end

run_queries(gql_wrapper)

-- clean up
-- --------

testdata.drop_spaces()

os.exit()

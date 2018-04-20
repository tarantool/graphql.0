#!/usr/bin/env tarantool

local utils = require('graphql.utils')
local test_utils = require('test.utils')
local yaml = require('yaml')
local json = require('json')
local fio = require('fio')
local http = require('http.client').new()
local graphql = require('graphql')
local testdata = require('test.testdata.common_testdata')


package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

box.cfg{background = false}
require('strict').on()


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

local accessor = graphql.accessor_space.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
})

local gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
})

-- test server
utils.show_trace(function()
    gql_wrapper:start_server()

    local method = 'POST'
    local url = "http://127.0.0.1:8080/graphql"
    local request_data =
        [[{"query":"query user_by_order($order_id: String)]] ..
        [[{\n    order_collection(order_id: $order_id) ]] ..
        [[{\n      order_id\n      description\n    }\n}",]] ..
        [["variables":{"order_id": "order_id_1"},"operationName":"user_by_order"}]]

    local _, response = pcall(function()
        return http:request(method, url, request_data)
    end)

    local body = json.decode(response.body)

    test_utils.print_and_return(
        ('RESULT\n%s'):format(yaml.encode(body.data))
    )

    gql_wrapper:stop_server()

    -- add space formats and try default instance

    box.space.order_collection:format({{name='order_id', type='string'},
        {name='user_id', type='string'}, {name='description', type='string'}})


    graphql.start_server()

     _, response = pcall(function()
        return http:request(method, url, request_data)
    end)

    body = json.decode(response.body)

    test_utils.print_and_return(
        ('RESULT\n%s'):format(yaml.encode(body.data))
    )

    graphql.stop_server()


end)

testdata.drop_spaces()

os.exit()

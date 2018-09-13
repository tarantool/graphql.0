#!/usr/bin/env tarantool

local fio = require('fio')

package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local json = require('json')
local http = require('http.client').new()
local graphql = require('graphql')
local utils = require('graphql.utils')
local test_utils = require('test.test_utils')
local vb = require('test.virtual_box')
local testdata = require('test.testdata.common_testdata')

box.cfg{background = false}

testdata.init_spaces()

-- upload test data
local meta = testdata.meta or testdata.get_test_metadata()
local virtbox = vb.get_virtbox_for_accessor('space', {meta = meta})
testdata.fill_test_data(virtbox)

-- acquire metadata
local metadata = testdata.get_test_metadata()
local schemas = metadata.schemas
local collections = metadata.collections
local service_fields = metadata.service_fields
local indexes = metadata.indexes

-- build accessor and graphql schemas
-- ----------------------------------

local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
    accessor = 'space',
}, test_utils.test_conf_graphql_opts()))

local test = tap.test('server')
test:plan(6)

-- test server
test_utils.show_trace(function()
    local res = gql_wrapper:start_server()
    local exp_res_start = 'The GraphQL server started at http://127.0.0.1:8080'
    test:is(res, exp_res_start, 'start_server')

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
    local exp_body_data = yaml.decode(([[
        --- {'order_collection': [{'description': 'first order of Ivan', 'order_id': 'order_id_1'}]}
    ]]):strip())
    test:is_deeply(body.data, exp_body_data, '1')

    local res = gql_wrapper:stop_server()
    local exp_res_stop = 'The GraphQL server stopped at http://127.0.0.1:8080'
    test:is(res, exp_res_stop, 'stop_server')

    -- add space formats and try default instance

    box.space.order_collection:format({{name='order_id', type='string'},
        {name='user_id', type='string'}, {name='description', type='string'}})

    local res = graphql.start_server(nil, nil,
        test_utils.test_conf_graphql_opts())
    test:is(res, exp_res_start, 'start_server')

     _, response = pcall(function()
        return http:request(method, url, request_data)
    end)

    body = json.decode(response.body)
    local exp_body_data = yaml.decode(([[
        --- {'order_collection': [{'description': 'first order of Ivan', 'order_id': 'order_id_1'}]}
    ]]):strip())
    test:is_deeply(body.data, exp_body_data, '2')

    local res = graphql.stop_server()
    test:is(res, exp_res_stop, 'stop_server')
end)

assert(test:check(), 'check plan')

testdata.drop_spaces()

os.exit()

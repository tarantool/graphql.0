#!/usr/bin/env tarantool

local tap = require('tap')
local yaml = require('yaml')
local json = require('json')
local fio = require('fio')
local http = require('http.client').new()
local test_utils = require('test.test_utils')

package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

box.cfg{background = false}

box.schema.create_space('user_collection')
box.space.user_collection:create_index('user_id_index',
    {type = 'tree', unique = true, parts = {
        1, 'string'
    }}
)

box.space.user_collection:format({{name='user_id', type='string'},
                                  {name='name', type='string'}})

box.space.user_collection:replace(
    {'user_id_1', 'Ivan'})
box.space.user_collection:replace(
    {'user_id_2', 'Vasiliy'})

local gql_lib = require('graphql')

local query = [[
    query user_collection($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            name
        }
    }
]]

local test = tap.test('default_instance')
test:plan(5)

-- test require('graphql').compile(query)
test_utils.show_trace(function()
    local variables_1 = {user_id = 'user_id_1'}
    local compiled_query = gql_lib.compile(query)
    local result = compiled_query:execute(variables_1)
    local exp_result = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          name: Ivan
    ]]):strip())
    test:is_deeply(result, exp_result, '1')
end)

-- test require('graphql').execute(query)
test_utils.show_trace(function()
    local variables_2 = {user_id = 'user_id_2'}
    local result = gql_lib.execute(query, variables_2)
    local exp_result = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_2
          name: Vasiliy
    ]]):strip())
    test:is_deeply(result, exp_result, '2')
end)

-- test server
test_utils.show_trace(function()
    local res = gql_lib.start_server()
    local exp_res_start = 'The GraphQL server started at http://127.0.0.1:8080'
    test:is(res, exp_res_start, 'start_server')

    local method = 'POST'
    local url = "http://127.0.0.1:8080/graphql"
    local request_data =
        [[{"query":"query user_collection]] ..
        [[{\n    user_collection {\n      user_id\n      name\n    }\n}",]] ..
        [["variables":{},"operationName":"user_collection"}]]

    local _, response = pcall(function()
        return http:request(method, url, request_data)
    end)

    local body = json.decode(response.body)
    local exp_body_data = yaml.decode(([[
        --- {'user_collection': [{'user_id': 'user_id_1', 'name': 'Ivan'}, {'user_id': 'user_id_2',
              'name': 'Vasiliy'}]}
    ]]):strip())

    test:is_deeply(body.data, exp_body_data, '3')

    local res = gql_lib.stop_server()
    local exp_res_stop = 'The GraphQL server stopped at http://127.0.0.1:8080'
    test:is(res, exp_res_stop, 'stop_server')
end)

assert(test:check(), 'check plan')

box.space.user_collection:drop()

os.exit()

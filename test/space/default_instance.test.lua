#!/usr/bin/env tarantool

local utils = require('graphql.utils')
local test_utils = require('test.utils')
local yaml = require('yaml')
local json = require('json')
local fio = require('fio')
local http = require('http.client').new()

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

-- test require('graphql').compile(query)
utils.show_trace(function()
    local variables_1 = {user_id = 'user_id_1'}
    local compiled_query = gql_lib.compile(query)
    local result = compiled_query:execute(variables_1)
    test_utils.print_and_return(
        ('RESULT\n%s'):format(yaml.encode(result)))
end)

-- test require('graphql').execute(query)
utils.show_trace(function()
    local variables_2 = {user_id = 'user_id_2'}
    local result = gql_lib.execute(query, variables_2)
    test_utils.print_and_return(
        ('RESULT\n%s'):format(yaml.encode(result)))
end)

-- test server
utils.show_trace(function()
    local res = gql_lib.start_server()
    print(res .. '\n')

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

    test_utils.print_and_return(
        ('RESULT\n%s'):format(yaml.encode(body.data))
    )

    gql_lib.stop_server()
end)

box.space.user_collection:drop()

os.exit()

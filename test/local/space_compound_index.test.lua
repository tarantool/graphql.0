#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local json = require('json')
local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')

local schemas = json.decode([[{
    "user": {
        "type": "record",
        "name": "user",
        "fields": [
            { "name": "user_str", "type": "string" },
            { "name": "user_num", "type": "long" },
            { "name": "first_name", "type": "string" },
            { "name": "last_name", "type": "string" }
        ]
    },
    "order": {
        "type": "record",
        "name": "order",
        "fields": [
            { "name": "order_str", "type": "string" },
            { "name": "order_num", "type": "long" },
            { "name": "user_str", "type": "string" },
            { "name": "user_num", "type": "long" },
            { "name": "description", "type": "string" }
        ]
    }
}]])

local collections = json.decode([[{
    "user_collection": {
        "schema_name": "user",
        "connections": [
            {
                "type": "1:N",
                "name": "order_connection",
                "destination_collection": "order_collection",
                "parts": [
                    { "source_field": "user_str", "destination_field": "user_str" },
                    { "source_field": "user_num", "destination_field": "user_num" }
                ],
                "index_name": "user_str_num_index"
            },
            {
                "type": "1:N",
                "name": "order_str_connection",
                "destination_collection": "order_collection",
                "parts": [
                    { "source_field": "user_str", "destination_field": "user_str" }
                ],
                "index_name": "user_str_num_index"
            }
        ]
    },
    "order_collection": {
        "schema_name": "order",
        "connections": [
            {
                "type": "1:1",
                "name": "user_connection",
                "destination_collection":  "user_collection",
                "parts": [
                    { "source_field": "user_str", "destination_field": "user_str" },
                    { "source_field": "user_num", "destination_field": "user_num" }
                ],
                "index_name": "user_str_num_index"
            }
        ]
    }
}]])

local service_fields = {
    user = {},
    order = {},
}

local indexes = {
    user_collection = {
        user_str_num_index = {
            service_fields = {},
            fields = {'user_str', 'user_num'},
            index_type = 'tree',
            unique = true,
            primary = true,
        },
    },
    order_collection = {
        order_str_num_index = {
            service_fields = {},
            fields = {'order_str', 'order_num'},
            index_type = 'tree',
            unique = true,
            primary = true,
        },
        user_str_num_index = {
            service_fields = {},
            fields = {'user_str', 'user_num'},
            index_type = 'tree',
            unique = false,
            primary = false,
        },
    },
}

-- fill spaces
-- -----------

-- user_collection fields
local U_USER_STR_FN = 1
local U_USER_NUM_FN = 2

-- order_collection fields
local O_ORDER_STR_FN = 1
local O_ORDER_NUM_FN = 2
local O_USER_STR_FN = 3
local O_USER_NUM_FN = 4

box.cfg{background = false}
box.once('test_space_init_spaces', function()
    -- users
    box.schema.create_space('user_collection')
    box.space.user_collection:create_index('user_str_num_index',
        {type = 'tree', unique = true, parts = {
            U_USER_STR_FN, 'string', U_USER_NUM_FN, 'unsigned',
        }}
    )

    -- orders
    box.schema.create_space('order_collection')
    box.space.order_collection:create_index('order_str_num_index',
        {type = 'tree', unique = true, parts = {
            O_ORDER_STR_FN, 'string', O_ORDER_NUM_FN, 'unsigned',
        }}
    )
    box.space.order_collection:create_index('user_str_num_index',
        {type = 'tree', unique = false, parts = {
            O_USER_STR_FN, 'string', O_USER_NUM_FN, 'unsigned',
        }}
    )
end)

for i = 1, 20 do
    for j = 1, 5 do
        local s =
            j % 5 == 1 and 'a' or
            j % 5 == 2 and 'b' or
            j % 5 == 3 and 'c' or
            j % 5 == 4 and 'd' or
            j % 5 == 0 and 'e' or
            nil
        assert(s ~= nil, 's must not be nil')
        box.space.user_collection:replace(
            {'user_str_' .. s, i, 'first name ' .. s, 'last name ' .. s})
        for k = 1, 10 do
            box.space.order_collection:replace(
                {'order_id_' .. s, i * 100 + k, 'user_str_' .. s, i,
                'description ' .. s})
        end
    end
end

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

-- get a top-level object by a full compound primary key
-- -----------------------------------------------------

local query_1 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
        }
    }
]]

local gql_query_1 = gql_wrapper:compile(query_1)

utils.show_trace(function()
    local variables_1_1 = {user_str = 'user_str_b', user_num = 12}
    local result = gql_query_1:execute(variables_1_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- select top-level objects by a partial compound primary key (or maybe use
-- fullscan)
-- ------------------------------------------------------------------------

utils.show_trace(function()
    local variables_1_2 = {user_num = 12}
    local result = gql_query_1:execute(variables_1_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- select objects by a connection by a full compound index
-- -------------------------------------------------------

local query_2 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
            order_connection {
                order_str
                order_num
                description
            }
        }
    }
]]

utils.show_trace(function()
    local gql_query_2 = gql_wrapper:compile(query_2)
    local variables_2 = {user_str = 'user_str_b', user_num = 12}
    local result = gql_query_2:execute(variables_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- select object by a connection by a partial compound index
-- ---------------------------------------------------------

local query_3 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
            order_str_connection {
                order_str
                order_num
                description
            }
        }
    }
]]

utils.show_trace(function()
    local gql_query_3 = gql_wrapper:compile(query_3)
    local variables_3 = {user_str = 'user_str_b', user_num = 12}
    local result = gql_query_3:execute(variables_3)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- clean up
box.space._schema:delete('oncetest_space_init_spaces')
box.space.user_collection:drop()
box.space.order_collection:drop()

os.exit()

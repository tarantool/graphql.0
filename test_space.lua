#!/usr/bin/env tarantool

local json = require('json')
local yaml = require('yaml')
local utils = require('utils')
local accessor_space = require('accessor_space')
local tarantool_graphql = require('tarantool_graphql')

local schemas = json.decode([[{
    "user": {
        "type": "record",
        "name": "user",
        "fields": [
            { "name": "user_id", "type": "string" },
            { "name": "first_name", "type": "string" },
            { "name": "last_name", "type": "string" }
        ]
    },
    "order": {
        "type": "record",
        "name": "order",
        "fields": [
            { "name": "order_id", "type": "string" },
            { "name": "user_id", "type": "string" },
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
                "destination_collection":  "order_collection",
                "parts": [
                    { "source_field": "user_id", "destination_field": "user_id" }
                ]
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
                    { "source_field": "user_id", "destination_field": "user_id" }
                ]
            }
        ]
    }
}]])

local service_fields = {
    user = {
--        {name = 'expires_on', type = 'long', default = 0},
    },
    order = {},
}

local indexes = {
    user_collection = {
        user_id_index = {
            service_fields = {},
            fields = {'user_id'},
            index_type = 'tree',
            unique = true,
        },
    },
    order_collection = {
        order_id_index = {
            service_fields = {},
            fields = {'order_id'},
            index_type = 'tree',
            unique = true,
        },
        user_id_index = {
            service_fields = {},
            fields = {'user_id'},
            index_type = 'tree',
            unique = false,
        },
    },
}

-- XXX: define certain format
local arguments = {}

-- fill spaces
-- -----------

-- user_collection fields
local U_USER_ID_FN = 1

-- order_collection fields
local O_ORDER_ID_FN = 1
local O_USER_ID_FN = 2

box.cfg{background = false}
box.once('test_space_init_spaces', function()
    box.schema.create_space('user_collection')
    box.space.user_collection:create_index('user_id_index',
        {type = 'tree', unique = true, parts = {
            U_USER_ID_FN, 'string'
        }}
    )
    box.schema.create_space('order_collection')
    box.space.order_collection:create_index('order_id_index',
        {type = 'tree', parts = {
            O_ORDER_ID_FN, 'string'
        }}
    )
    box.space.order_collection:create_index('user_id_index',
        {type = 'tree', unique = false, parts = {
            O_USER_ID_FN, 'string'
        }}
    )
end)

box.space.user_collection:replace(
    {'user_id_1', 'Ivan', 'Ivanov'})
box.space.user_collection:replace(
    {'user_id_2', 'Vasiliy', 'Pupkin'})
box.space.order_collection:replace(
    {'order_id_1', 'user_id_1', 'first order of Ivan'})
box.space.order_collection:replace(
    {'order_id_2', 'user_id_1', 'second order of Ivan'})
box.space.order_collection:replace(
    {'order_id_3', 'user_id_2', 'first order of Vasiliy'})

-- build accessor and graphql schemas
-- ----------------------------------

local accessor = accessor_space.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
    arguments = arguments,
})

local gql_wrapper = tarantool_graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
})

-- run queries
-- -----------

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

utils.show_trace(function()
    local variables_1 = {order_id = 'order_id_1'}
    local gql_query_1 = gql_wrapper:compile(query_1)
    local result = gql_query_1:execute(variables_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

local query_2 = [[
    query user_order($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            last_name
            first_name
            order_connection {
                order_id
                description
            }
        }
    }
]]

utils.show_trace(function()
    local variables_2 = {user_id = 'user_id_1'}
    local gql_query_2 = gql_wrapper:compile(query_2)
    local result = gql_query_2:execute(variables_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

os.exit()

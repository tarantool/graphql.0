#!/usr/bin/env tarantool

--local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')
local graphql = require('graphql')


local function print_and_return(...)
    print(...)
    return table.concat({...}, ' ') .. '\n'
end

--local schemas = json.decode([[{
--    "user": {
--        "type": "record",
--        "name": "user",
--        "fields": [
--            { "name": "user_id", "type": "string" },
--            { "name": "name", "type": "string*" },
--            { "name": "age", "type": "int*" },
--            { "name": "location", "type":
--                { "type":"record", "fields": [
--                        { "name": "x", "type": "int"},
--                        { "name": "y", "type": "int"}
--                    ]
--                }
--            }
--        ]
--    },
--    "order": {
--        "type": "record",
--        "name": "order",
--        "fields": [
--            { "name": "order_id", "type": "string" },
--            { "name": "user_id", "type": "string" },
--            { "name": "description", "type": "string" }
--        ]
--    }
--}]])

--local service_fields = {
--    user = {
--        {name = 'expires_on', type = 'long', default = 0},
--    },
--    order = {},
--}
--
--local indexes = {
--    user_collection = {
--        user_id_index = {
--            service_fields = {},
--            fields = {'user_id'},
--            index_type = 'tree',
--            unique = true,
--            primary = true,
--        },
--    },
--    order_collection = {
--        order_id_index = {
--            service_fields = {},
--            fields = {'order_id'},
--            index_type = 'tree',
--            unique = true,
--            primary = true,
--        },
--        user_id_index = {
--            service_fields = {},
--            fields = {'user_id'},
--            index_type = 'tree',
--            unique = false,
--            primary = false,
--        },
--    },
--}


local function init_spaces()

    -- user_collection fields
    local U_USER_ID_FN = 1

    -- order_collection fields
    local O_ORDER_ID_FN = 1
    local O_USER_ID_FN = 2

    box.once('test_space_init_spaces', function()
        box.schema.create_space('user_collection')
        box.space.user_collection:create_index('user_id_index',
        {type = 'tree', unique = true, parts = {
            U_USER_ID_FN, 'string'
        }}
        )

        box.space.user_collection:create_index('user_name_index',
        {type = 'tree', unique = true, parts = {
            U_USER_ID_FN + 1, 'string'
        }}
        )

        box.space.user_collection:format({{name='user_id', type='string'},
            {name='name', type='string'},
            {name='age', type='integer', is_nullable=true},
            {name='location', type='integer', is_nullable=true},})

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
        box.space.order_collection:format({{name='order_id', type='string'},
            {name='user_id', type='string'},
            {name='description', type='string'}})
    end)
end

local function fill_test_data(shard)
    local shard = shard or box.space

    shard.user_collection:replace(
    {'user_id_1', 'Ivan', 42})
    shard.user_collection:replace(
    {'user_id_2', 'Vasiliy'})
    shard.order_collection:replace(
    {'order_id_1', 'user_id_1', 'first order of Ivan'})
    shard.order_collection:replace(
    {'order_id_2', 'user_id_1', 'second order of Ivan'})
    shard.order_collection:replace(
    {'order_id_3', 'user_id_2', 'first order of Vasiliy'})
end

local function drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
    box.space.order_collection:drop()
end


local function run_queries(gql_wrapper)
    local results = ''

    local query_1 = [[
        query user_order($user_id: String) {
            user_collection(user_id: $user_id) {
                user_id
                age
                name
            }
        }
    ]]

    utils.show_trace(function()
        local variables_1 = {user_id = 'user_id_1', }
        local gql_query_1 = gql_wrapper:compile(query_1)
        local result = gql_query_1:execute(variables_1)
        results = results .. print_and_return(
        ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    return results
end

utils.show_trace(function()
    box.cfg { background = false }
    init_spaces()
    fill_test_data()
    local gql_wrapper = graphql.new()
    run_queries(gql_wrapper)
    --@todo check that schema is the same
    --utils.is_subtable(gql_wrapper.state.schema)
    drop_spaces()
end)

os.exit()
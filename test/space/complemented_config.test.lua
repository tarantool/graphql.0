#!/usr/bin/env tarantool

--local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')
local graphql = require('graphql')

local connections = {
    {
        name='order_connection',
        source_collection = 'user_collection',
        destination_collection = 'order_collection',
        index_name = 'user_id_index'
    }
}

local function init_spaces()
    box.once('test_space_init_spaces', function()
        box.schema.create_space('user_collection')
        box.space.user_collection:format({{name='user_id', type='string'},
                                          {name='name', type='string'},
                                          {name='age', type='integer', is_nullable=true}})
        box.space.user_collection:create_index('user_id_index',
            {type = 'tree', unique = true, parts = { 1, 'string' }})

        box.schema.create_space('order_collection')
        box.space.order_collection:format({{name='order_id', type='string'},
                                           {name='user_id', type='string'},
                                           {name='description', type='string'}})
        box.space.order_collection:create_index('order_id_index',
            {type = 'tree', parts = { 1, 'string' }})
        box.space.order_collection:create_index('user_id_index',
            {type = 'tree', parts = { 2, 'string' }})
    end)
end

local function fill_test_data(shard)
    local shard = shard or box.space

    shard.user_collection:replace(
        {'user_id_1', 'Ivan', 42})
    shard.user_collection:replace(
        {'user_id_2', 'Vasiliy'})

    shard.order_collection:replace(
        {'order_id_1', 'user_id_1', 'Ivan order'})
    shard.order_collection:replace(
        {'order_id_2', 'user_id_2', 'Vasiliy order'})
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
                order_connection{
                    order_id
                    description
                }
            }
        }
    ]]

    local variables_1 = {user_id = 'user_id_1'}
    local gql_query_1 = gql_wrapper:compile(query_1)
    local result = gql_query_1:execute(variables_1)
    results = results .. ('RESULT\n%s'):format(yaml.encode(result))

    local cfg = gql_wrapper.internal.cfg
    cfg.accessor = nil
    local result = cfg
    results = results .. ('RESULT\n%s'):format(yaml.encode(result))

    return results
end

utils.show_trace(function()
    box.cfg { background = false }
    init_spaces()
    fill_test_data()
    local gql_wrapper = graphql.new({connections = connections})
    print(run_queries(gql_wrapper))
    drop_spaces()
end)

os.exit()

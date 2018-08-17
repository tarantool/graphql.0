#!/usr/bin/env tarantool

local tap = require('tap')
local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')
local test_utils = require('test.test_utils')

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
    local test = tap.test('complemented_config')
    test:plan(2)

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

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local variables_1_1 = {user_id = 'user_id_1'}
    local result_1_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)
    local exp_result_1_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          age: 42
          name: Ivan
          order_connection:
            order_id: order_id_1
            description: Ivan order
    ]]):strip())
    test:is_deeply(result_1_1.data, exp_result_1_1, '1_1')

    local cfg = gql_wrapper.internal.cfg
    cfg.accessor = nil
    cfg.e_schemas = nil
    local result_1_2 = cfg
    local exp_result_1_2 = yaml.decode(([[
        ---
        schemas:
          user_collection:
            type: record
            name: user_collection
            fields:
            - name: user_id
              type: string
            - name: name
              type: string
            - name: age
              type: long*
          order_collection:
            type: record
            name: order_collection
            fields:
            - name: order_id
              type: string
            - name: user_id
              type: string
            - name: description
              type: string
        connections:
        - index_name: user_id_index
          destination_collection: order_collection
          name: order_connection
          source_collection: user_collection
        indexes:
          user_collection:
            user_id_index:
              unique: true
              primary: true
              service_fields: []
              fields:
              - user_id
              index_type: tree
          order_collection:
            order_id_index:
              unique: true
              primary: true
              service_fields: []
              fields:
              - order_id
              index_type: tree
            user_id_index:
              unique: true
              primary: false
              service_fields: []
              fields:
              - user_id
              index_type: tree
        collections:
          user_collection:
            schema_name: user_collection
            connections:
            - destination_collection: order_collection
              parts:
              - source_field: user_id
                destination_field: user_id
              type: 1:1
              index_name: user_id_index
              name: order_connection
              source_collection: user_collection
            name: user_collection
          order_collection:
            schema_name: order_collection
            connections: []
            name: order_collection
        collection_use_tomap:
          user_collection: true
          order_collection: true
        service_fields:
          user_collection: []
          order_collection: []
    ]]):strip())
    exp_result_1_2 = utils.merge_tables(exp_result_1_2,
        test_utils.test_conf_graphql_opts())
    test:is_deeply(result_1_2, exp_result_1_2, '1_2')

    assert(test:check(), 'check plan')
end

test_utils.show_trace(function()
    box.cfg { background = false }
    init_spaces()
    fill_test_data()
    local gql_wrapper = graphql.new(utils.merge_tables(
        {connections = connections}, test_utils.test_conf_graphql_opts()))
    run_queries(gql_wrapper)
    drop_spaces()
end)

os.exit()

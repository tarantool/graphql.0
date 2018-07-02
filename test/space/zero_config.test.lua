#!/usr/bin/env tarantool

local tap = require('tap')
local yaml = require('yaml')
local graphql = require('graphql')
local test_utils = require('test.test_utils')

local function init_spaces()
    local U_USER_ID_FN = 1

    box.once('test_space_init_spaces', function()
        box.schema.create_space('user_collection')
        box.space.user_collection:create_index('user_id_index',
            {type = 'tree', unique = true, parts = {
                U_USER_ID_FN, 'string'
            }}
        )

        box.space.user_collection:format(
            {{name='user_id', type='string'}, {name='name', type='string'},
            {name='age', type='integer', is_nullable=true}}
        )
    end)
end

local function fill_test_data(shard)
    local shard = shard or box.space

    shard.user_collection:replace(
        {'user_id_1', 'Ivan', 42})
    shard.user_collection:replace(
        {'user_id_2', 'Vasiliy'})
end

local function drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
end

local function run_queries(gql_wrapper)
    local test = tap.test('zero_config')
    test:plan(2)

    local query_1 = [[
        query user_order($user_id: String) {
            user_collection(user_id: $user_id) {
                user_id
                age
                name
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local result_1_1 = test_utils.show_trace(function()
        local variables_1_1 = {user_id = 'user_id_1'}
        return gql_query_1:execute(variables_1_1)
    end)

    local exp_result_1_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          age: 42
          name: Ivan
    ]]):strip())
    test:is_deeply(result_1_1.data, exp_result_1_1, '1_1')

    local result_1_2 = test_utils.show_trace(function()
        local cfg = gql_wrapper.internal.cfg
        cfg.accessor = nil
        cfg.e_schemas = nil
        return cfg
    end)

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
        connections: []
        indexes:
          user_collection:
            user_id_index:
              unique: true
              primary: true
              service_fields: []
              fields:
              - user_id
              index_type: tree
        collections:
          user_collection:
            schema_name: user_collection
            connections: []
            name: user_collection
        collection_use_tomap:
          user_collection: true
        service_fields:
          user_collection: []
    ]]):strip())
    test:is_deeply(result_1_2, exp_result_1_2, '1_2')

    assert(test:check(), 'check plan')
end

test_utils.show_trace(function()
    box.cfg { background = false }
    init_spaces()
    fill_test_data()
    local gql_wrapper = graphql.new()
    run_queries(gql_wrapper)
    drop_spaces()
end)

os.exit()

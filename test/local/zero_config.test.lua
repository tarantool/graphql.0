#!/usr/bin/env tarantool

--local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')
local graphql = require('graphql')


local function print_and_return(...)
    print(...)
    return table.concat({...}, ' ') .. '\n'
end

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
        local variables_1 = {user_id = 'user_id_1'}
        local gql_query_1 = gql_wrapper:compile(query_1)
        local result = gql_query_1:execute(variables_1)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    utils.show_trace(function()
        local cfg = gql_wrapper.internal.cfg
        cfg.accessor = nil
        local result = cfg
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
    drop_spaces()
end)

os.exit()

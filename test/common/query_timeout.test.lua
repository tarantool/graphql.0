#!/usr/bin/env tarantool
local multirunner = require('multirunner')
local data = require('test_data_user_order')
local test_run = require('test_run').new()
local tap = require('tap')

box.cfg({})
local test = tap.test('result cnt')
test:plan(3)

local function run(setup_name, shard, graphql)
    print(setup_name)
    local accessor_class
    local virtbox
    -- SHARD
    if shard ~= nil then
        accessor_class = graphql.accessor_shard
        virtbox = shard
    else
        accessor_class = graphql.accessor_space
        virtbox = box.space
    end
    local accessor = accessor_class.new({
        schemas = data.meta.schemas,
        collections = data.meta.collections,
        service_fields = data.meta.service_fields,
        indexes = data.meta.indexes,
        timeout_ms = 0.001
    })

    local gql_wrapper = graphql.new({
        schemas = data.meta.schemas,
        collections = data.meta.collections,
        accessor = accessor,
    })
    data.fill_test_data(virtbox)
    local query = [[
        query object_result_max {
            user_collection {
                id
                last_name
                first_name
                order_connection {
                    id
                    user_id
                    description
                }
            }
        }
    ]]

    local gql_query = gql_wrapper:compile(query)
    local variables = {
    }
    local ok, result = pcall(gql_query.execute, gql_query, variables)
    assert(ok == false, 'this test should fail')
    test:like(result, 'query execution timeout exceeded', 'timeout test')

end

multirunner.run(test_run,
                data.init_spaces,
                data.drop_spaces,
                run)

os.exit(test:check() == true and 0 or 1)

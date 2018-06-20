local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local array_testdata = {}

function array_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "user": {
            "name": "user",
            "type": "record",
            "fields": [
                { "name": "user_id", "type": "string" },
                { "name": "favorite_food", "type": {"type": "array", "items": "string"} },
                { "name": "favorite_holidays", "type": {"type": "map", "values": "string"} },
                { "name": "user_balances", "type":
                    {"type": "array", "items":
                        {   "name": "balance",
                            "type": "record",
                            "fields": [{
                                "name": "value",
                                "type": "int"
                            }]
                        }
                    }
                },
                { "name": "customer_balances", "type":
                    {"type": "map", "values":
                        {   "name": "another_balance",
                            "type": "record",
                            "fields": [{
                                "name": "value",
                                "type": "int"
                            }]
                        }
                    }
                }
            ]
        }
    }]])

    local collections = json.decode([[{
        "user_collection": {
            "schema_name": "user",
            "connections": []
        }
    }]])

    local service_fields = {
        user = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        order = {},
    }

    local indexes = {
        user_collection = {
            user_id_index = {
                service_fields = {},
                fields = { 'user_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        }
    }

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function array_testdata.init_spaces()
    -- user_collection fields
    local U_USER_ID_FN = 2

    box.once('test_space_init_spaces', function()
        box.schema.create_space('user_collection')
        box.space.user_collection:create_index('user_id_index',
            { type = 'tree', unique = true, parts = { U_USER_ID_FN, 'string' }}
        )
    end)
end

function array_testdata.fill_test_data(shard)
    local shard = shard or box.space

    shard.user_collection:replace(
        { 1827767717, 'user_id_1', { 'meat', 'potato' },
        { december = 'new year', march = 'vacation' },
          { { 33 }, { 44 } },
          { salary = { 333 }, deposit = { 444 } }
        })
    --@todo add empty array
end

function array_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
end

function array_testdata.run_queries(gql_wrapper)
    local test = tap.test('array_and_map')
    test:plan(1)

    local query_1 = [[
        query user_holidays($user_id: String) {
            user_collection(user_id: $user_id) {
                user_id
                favorite_food
                favorite_holidays
                user_balances {
                    value
                }
                customer_balances
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)
    local variables_1 = { user_id = 'user_id_1' }
    local result_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1)
    end)

    local exp_result_1 = yaml.decode(([[
        ---
        user_collection:
        - favorite_holidays: {'december': 'new year', 'march': 'vacation'}
          user_id: user_id_1
          user_balances:
          - value: 33
          - value: 44
          favorite_food:
          - meat
          - potato
          customer_balances: {'salary': {'value': 333}, 'deposit': {'value': 444}}
    ]]):strip())

    test:is_deeply(result_1.data, exp_result_1, '1')

    assert(test:check(), 'check plan')
end

return array_testdata

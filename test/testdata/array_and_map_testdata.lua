local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

local array_testdata = {}

local function print_and_return(...)
    print(...)
    return table.concat({ ... }, ' ') .. '\n'
end

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
            { type = 'tree', unique = true, parts = {
                U_USER_ID_FN, 'string'
            } }
        )
    end)
end

function array_testdata.fill_test_data(shard)
    local shard = shard or box.space

    shard.user_collection:replace(
        { 1827767717, 'user_id_1', { 'meat', 'potato' },
        { december = 'new year', march = 'vacation' },
          { { 33 }, { 44 } },
          { salary = {333}, deposit = { 444}}
        })
    --@todo add empty array
end

function array_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
end

function array_testdata.run_queries(gql_wrapper)

    local results = ''

    local query = [[
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

    utils.show_trace(function()
        local variables_1 = { user_id = 'user_id_1' }
        local gql_query_1 = gql_wrapper:compile(query)
        local result = gql_query_1:execute(variables_1)
        results = results .. print_and_return(
        ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    return results
end

return array_testdata

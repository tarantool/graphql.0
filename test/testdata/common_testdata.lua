local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

local common_testdata = {}

function common_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                { "name": "user_id", "type": "string" },
                { "name": "first_name", "type": "string" },
                { "name": "middle_name", "type": "string*" },
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
                    ],
                    "index_name": "user_id_index"
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
                    ],
                    "index_name": "user_id_index"
                }
            ]
        }
    }]])

    local service_fields = {
        user = {
            {name = 'expires_on', type = 'long', default = 0},
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
                primary = true,
            },
        },
        order_collection = {
            order_id_index = {
                service_fields = {},
                fields = {'order_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            user_id_index = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = false,
                primary = false,
            },
        },
    }

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function common_testdata.init_spaces()
    -- user_collection fields
    local U_USER_ID_FN = 2

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
end

function common_testdata.fill_test_data(shard)
    local shard = shard or box.space

    local NULL_T = 0
    local STRING_T = 1

    shard.user_collection:replace(
        {1827767717, 'user_id_1', 'Ivan', STRING_T, 'Ivanovich', 'Ivanov'})
    shard.user_collection:replace(
        {1827767717, 'user_id_2', 'Vasiliy', NULL_T, box.NULL, 'Pupkin'})
    shard.order_collection:replace(
        {'order_id_1', 'user_id_1', 'first order of Ivan'})
    shard.order_collection:replace(
        {'order_id_2', 'user_id_1', 'second order of Ivan'})
    shard.order_collection:replace(
        {'order_id_3', 'user_id_2', 'first order of Vasiliy'})

    for i = 3, 100 do
        local s = tostring(i)
        shard.user_collection:replace(
            {1827767717, 'user_id_' .. s, 'first name ' .. s, NULL_T, box.NULL,
            'last name ' .. s})
        for j = (4 + (i - 3) * 40), (4 + (i - 2) * 40) - 1 do
            local t = tostring(j)
            shard.order_collection:replace(
                {'order_id_' .. t, 'user_id_' .. s, 'order of user ' .. s})
        end
    end
end

function common_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
    box.space.order_collection:drop()
end

function common_testdata.run_queries(gql_wrapper)
    local test = tap.test('common')
    test:plan(10)

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

    local exp_result_1 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          user_connection:
            user_id: user_id_1
            last_name: Ivanov
            first_name: Ivan
    ]]):strip())

    utils.show_trace(function()
        local variables_1 = {order_id = 'order_id_1'}
        local gql_query_1 = gql_wrapper:compile(query_1)
        local result = gql_query_1:execute(variables_1)
        test:is_deeply(result, exp_result_1, '1')
    end)

    local query_2 = [[
        query user_order($user_id: String, $first_name: String, $limit: Int,
                $offset: String) {
            user_collection(user_id: $user_id, first_name: $first_name) {
                user_id
                last_name
                first_name
                order_connection(limit: $limit, offset: $offset) {
                    order_id
                    description
                }
            }
        }
    ]]

    local gql_query_2 = utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    local exp_result_2_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          last_name: Ivanov
          first_name: Ivan
          order_connection:
          - order_id: order_id_1
            description: first order of Ivan
          - order_id: order_id_2
            description: second order of Ivan
    ]]):strip())

    utils.show_trace(function()
        local variables_2_1 = {user_id = 'user_id_1'}
        local result = gql_query_2:execute(variables_2_1)
        test:is_deeply(result, exp_result_2_1, '2_1')
    end)

    local exp_result_2_2 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_42
          last_name: last name 42
          first_name: first name 42
          order_connection:
          - order_id: order_id_1574
            description: order of user 42
          - order_id: order_id_1575
            description: order of user 42
          - order_id: order_id_1576
            description: order of user 42
          - order_id: order_id_1577
            description: order of user 42
          - order_id: order_id_1578
            description: order of user 42
          - order_id: order_id_1579
            description: order of user 42
          - order_id: order_id_1580
            description: order of user 42
          - order_id: order_id_1581
            description: order of user 42
          - order_id: order_id_1582
            description: order of user 42
          - order_id: order_id_1583
            description: order of user 42
    ]]):strip())

    utils.show_trace(function()
        local variables_2_2 = {
            user_id = 'user_id_42',
            limit = 10,
            offset = 'order_id_1573', -- 10th
        }
        local result = gql_query_2:execute(variables_2_2)
        test:is_deeply(result, exp_result_2_2, '2_2')
    end)

    local exp_result_2_3 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_42
          last_name: last name 42
          first_name: first name 42
          order_connection:
          - order_id: order_id_1602
            description: order of user 42
          - order_id: order_id_1603
            description: order of user 42
    ]]):strip())

    utils.show_trace(function()
        local variables_2_3 = {
            user_id = 'user_id_42',
            limit = 10,
            offset = 'order_id_1601', -- 38th
        }
        local result = gql_query_2:execute(variables_2_3)
        test:is_deeply(result, exp_result_2_3, '2_3')
    end)

    local exp_result_2_4 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_42
          last_name: last name 42
          first_name: first name 42
          order_connection:
          - order_id: order_id_1603
            description: order of user 42
    ]]):strip())

    utils.show_trace(function()
        local variables_2_4 = {
            first_name = 'first name 42',
            limit = 3,
            offset = 'order_id_1602', -- 39th
        }
        local result = gql_query_2:execute(variables_2_4)
        test:is_deeply(result, exp_result_2_4, '2_4')
    end)

    local exp_result_2_5 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_42
          last_name: last name 42
          first_name: first name 42
          order_connection:
          - order_id: order_id_1564
            description: order of user 42
          - order_id: order_id_1565
            description: order of user 42
          - order_id: order_id_1566
            description: order of user 42
          - order_id: order_id_1567
            description: order of user 42
          - order_id: order_id_1568
            description: order of user 42
          - order_id: order_id_1569
            description: order of user 42
          - order_id: order_id_1570
            description: order of user 42
          - order_id: order_id_1571
            description: order of user 42
          - order_id: order_id_1572
            description: order of user 42
          - order_id: order_id_1573
            description: order of user 42
          - order_id: order_id_1574
            description: order of user 42
          - order_id: order_id_1575
            description: order of user 42
          - order_id: order_id_1576
            description: order of user 42
          - order_id: order_id_1577
            description: order of user 42
          - order_id: order_id_1578
            description: order of user 42
          - order_id: order_id_1579
            description: order of user 42
          - order_id: order_id_1580
            description: order of user 42
          - order_id: order_id_1581
            description: order of user 42
          - order_id: order_id_1582
            description: order of user 42
          - order_id: order_id_1583
            description: order of user 42
          - order_id: order_id_1584
            description: order of user 42
          - order_id: order_id_1585
            description: order of user 42
          - order_id: order_id_1586
            description: order of user 42
          - order_id: order_id_1587
            description: order of user 42
          - order_id: order_id_1588
            description: order of user 42
          - order_id: order_id_1589
            description: order of user 42
          - order_id: order_id_1590
            description: order of user 42
          - order_id: order_id_1591
            description: order of user 42
          - order_id: order_id_1592
            description: order of user 42
          - order_id: order_id_1593
            description: order of user 42
          - order_id: order_id_1594
            description: order of user 42
          - order_id: order_id_1595
            description: order of user 42
          - order_id: order_id_1596
            description: order of user 42
          - order_id: order_id_1597
            description: order of user 42
          - order_id: order_id_1598
            description: order of user 42
          - order_id: order_id_1599
            description: order of user 42
          - order_id: order_id_1600
            description: order of user 42
          - order_id: order_id_1601
            description: order of user 42
          - order_id: order_id_1602
            description: order of user 42
          - order_id: order_id_1603
            description: order of user 42
    ]]):strip())

    -- no limit, no offset
    utils.show_trace(function()
        local variables_2_5 = {user_id = 'user_id_42'}
        local result = gql_query_2:execute(variables_2_5)
        test:is_deeply(result, exp_result_2_5, '2_5')
    end)

    local query_3 = [[
        query users($limit: Int, $offset: String) {
            user_collection(limit: $limit, offset: $offset) {
                user_id
                last_name
                first_name
            }
        }
    ]]

    local exp_result_3 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_54
          last_name: last name 54
          first_name: first name 54
        - user_id: user_id_55
          last_name: last name 55
          first_name: first name 55
        - user_id: user_id_56
          last_name: last name 56
          first_name: first name 56
        - user_id: user_id_57
          last_name: last name 57
          first_name: first name 57
        - user_id: user_id_58
          last_name: last name 58
          first_name: first name 58
        - user_id: user_id_59
          last_name: last name 59
          first_name: first name 59
        - user_id: user_id_6
          last_name: last name 6
          first_name: first name 6
        - user_id: user_id_60
          last_name: last name 60
          first_name: first name 60
        - user_id: user_id_61
          last_name: last name 61
          first_name: first name 61
        - user_id: user_id_62
          last_name: last name 62
          first_name: first name 62
    ]]):strip())

    utils.show_trace(function()
        local variables_3 = {
            limit = 10,
            offset = 'user_id_53', -- 50th (alphabetical sort)
        }
        local gql_query_3 = gql_wrapper:compile(query_3)
        local result = gql_query_3:execute(variables_3)
        test:is_deeply(result, exp_result_3, '3')
    end)

    -- extra filter for 1:N connection
    -- -------------------------------

    local query_4 = [[
        query user_order($first_name: String, $description: String) {
            user_collection(first_name: $first_name) {
                user_id
                last_name
                first_name
                order_connection(description: $description) {
                    order_id
                    description
                }
            }
        }
    ]]

    local gql_query_4 = utils.show_trace(function()
        return gql_wrapper:compile(query_4)
    end)

    local exp_result_4_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          last_name: Ivanov
          first_name: Ivan
          order_connection:
          - order_id: order_id_1
            description: first order of Ivan
    ]]):strip())

    -- should match 1 order
    utils.show_trace(function()
        local variables_4_1 = {
            first_name = 'Ivan',
            description = 'first order of Ivan',
        }
        local result = gql_query_4:execute(variables_4_1)
        test:is_deeply(result, exp_result_4_1, '4_1')
    end)

    local exp_result_4_2 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          last_name: Ivanov
          first_name: Ivan
          order_connection: []
    ]]):strip())

    -- should match no orders
    utils.show_trace(function()
        local variables_4_2 = {
            first_name = 'Ivan',
            description = 'non-existent order',
        }
        local result = gql_query_4:execute(variables_4_2)
        test:is_deeply(result, exp_result_4_2, '4_2')
    end)

    -- extra filter for 1:1 connection
    -- -------------------------------

    local query_5 = [[
        query user_by_order($first_name: String, $description: String) {
            order_collection(description: $description) {
                order_id
                description
                user_connection(first_name: $first_name) {
                    user_id
                    last_name
                    first_name
                }
            }
        }
    ]]

    local gql_query_5 = utils.show_trace(function()
        return gql_wrapper:compile(query_5)
    end)

    local exp_result_5_1 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          user_connection:
            user_id: user_id_1
            last_name: Ivanov
            first_name: Ivan
    ]]):strip())

    -- should match 1 user
    utils.show_trace(function()
        local variables_5_1 = {
            first_name = 'Ivan',
            description = 'first order of Ivan',
        }
        local result = gql_query_5:execute(variables_5_1)
        test:is_deeply(result, exp_result_5_1, '5_1')
    end)

    --[=[
    local exp_result_5_2 = yaml.decode(([[
        --- []
    ]]):strip())

    -- should match no users (or give an error?)
    utils.show_trace(function()
        local variables_5_2 = {
            first_name = 'non-existent user',
            description = 'first order of Ivan',
        }
        local result = gql_query_5:execute(variables_5_2)
        test:is_deeply(result, exp_result_5_2, '5_2')
    end)
    ]=]--

    assert(test:check(), 'check plan')
end

return common_testdata

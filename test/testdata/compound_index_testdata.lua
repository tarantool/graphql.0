local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local compound_index_testdata = {}

-- schemas and meta-information
-- ----------------------------

function compound_index_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                { "name": "user_str", "type": "string" },
                { "name": "user_num", "type": "long" },
                { "name": "first_name", "type": "string" },
                { "name": "last_name", "type": "string" }
            ]
        },
        "order": {
            "type": "record",
            "name": "order",
            "fields": [
                { "name": "order_str", "type": "string" },
                { "name": "order_num", "type": "long" },
                { "name": "user_str", "type": "string" },
                { "name": "user_num", "type": "long" },
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
                    "destination_collection": "order_collection",
                    "parts": [
                        { "source_field": "user_str", "destination_field": "user_str" },
                        { "source_field": "user_num", "destination_field": "user_num" }
                    ],
                    "index_name": "user_str_num_index"
                },
                {
                    "type": "1:N",
                    "name": "order_str_connection",
                    "destination_collection": "order_collection",
                    "parts": [
                        { "source_field": "user_str", "destination_field": "user_str" }
                    ],
                    "index_name": "user_str_num_index"
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
                        { "source_field": "user_str", "destination_field": "user_str" },
                        { "source_field": "user_num", "destination_field": "user_num" }
                    ],
                    "index_name": "user_str_num_index"
                }
            ]
        }
    }]])

    local service_fields = {
        user = {},
        order = {},
    }

    local indexes = {
        user_collection = {
            user_str_num_index = {
                service_fields = {},
                fields = {'user_str', 'user_num'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
        order_collection = {
            order_str_num_index = {
                service_fields = {},
                fields = {'order_str', 'order_num'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            user_str_num_index = {
                service_fields = {},
                fields = {'user_str', 'user_num'},
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

function compound_index_testdata.init_spaces()
    -- user_collection fields
    local U_USER_STR_FN = 1
    local U_USER_NUM_FN = 2

    -- order_collection fields
    local O_ORDER_STR_FN = 1
    local O_ORDER_NUM_FN = 2
    local O_USER_STR_FN = 3
    local O_USER_NUM_FN = 4

    box.once('test_space_init_spaces', function()
        -- users
        box.schema.create_space('user_collection')
        box.space.user_collection:create_index('user_str_num_index',
            {type = 'tree', unique = true, parts = {
                U_USER_STR_FN, 'string', U_USER_NUM_FN, 'unsigned',
            }}
        )

        -- orders
        box.schema.create_space('order_collection')
        box.space.order_collection:create_index('order_str_num_index',
            {type = 'tree', unique = true, parts = {
                O_ORDER_STR_FN, 'string', O_ORDER_NUM_FN, 'unsigned',
            }}
        )
        box.space.order_collection:create_index('user_str_num_index',
            {type = 'tree', unique = false, parts = {
                O_USER_STR_FN, 'string', O_USER_NUM_FN, 'unsigned',
            }}
        )
    end)
end

function compound_index_testdata.fill_test_data(shard)
    local shard = shard or box.space

    for i = 1, 20 do
        for j = 1, 5 do
            local s =
                j % 5 == 1 and 'a' or
                j % 5 == 2 and 'b' or
                j % 5 == 3 and 'c' or
                j % 5 == 4 and 'd' or
                j % 5 == 0 and 'e' or
                nil
            assert(s ~= nil, 's must not be nil')
            local user_str = 'user_str_' .. s
            local user_num = i
            shard.user_collection:replace(
                {user_str, user_num, 'first name ' .. s, 'last name ' .. s})
            for k = 1, 10 do
                local order_str = 'order_str_' .. s .. '_' .. tostring(k)
                local order_num = i * 100 + k
                shard.order_collection:replace(
                    {order_str, order_num, user_str, user_num,
                    'description ' .. s})
            end
        end
    end
end

function compound_index_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
    box.space.order_collection:drop()
end

function compound_index_testdata.run_queries(gql_wrapper)
    local test = tap.test('compound')
    test:plan(14)

    -- {{{ get a top-level object by a full compound primary key

    local query_1 = [[
        query users($user_str: String, $user_num: Long,
                $first_name: String) {
            user_collection(user_str: $user_str, user_num: $user_num
                    first_name: $first_name) {
                user_str
                user_num
                last_name
                first_name
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local result_1_1 = test_utils.show_trace(function()
        local variables_1_1 = {user_str = 'user_str_b', user_num = 12}
        return gql_query_1:execute(variables_1_1)
    end)

    local exp_result_1_1 = yaml.decode(([[
        ---
        user_collection:
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 12
    ]]):strip())

    test:is_deeply(result_1_1.data, exp_result_1_1, '1_1')

    -- }}}
    -- {{{ get a top-level object by a full compound primary key plus filter

    local result_1_2 = test_utils.show_trace(function()
        local variables_1_2 = {
            user_str = 'user_str_b',
            user_num = 12,
            first_name = 'non-existent',
        }
        return gql_query_1:execute(variables_1_2)
    end)

    local exp_result_1_2 = yaml.decode(([[
        ---
        user_collection: []
    ]]):strip())

    test:is_deeply(result_1_2.data, exp_result_1_2, '1_2')

    -- }}}
    -- {{{ select top-level objects by a partial compound primary key (or maybe
    -- use fullscan)

    local result_1_3 = test_utils.show_trace(function()
        local variables_1_3 = {user_num = 12}
        return gql_query_1:execute(variables_1_3)
    end)

    local exp_result_1_3 = yaml.decode(([[
        ---
        user_collection:
        - last_name: last name a
          user_str: user_str_a
          first_name: first name a
          user_num: 12
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 12
        - last_name: last name c
          user_str: user_str_c
          first_name: first name c
          user_num: 12
        - last_name: last name d
          user_str: user_str_d
          first_name: first name d
          user_num: 12
        - last_name: last name e
          user_str: user_str_e
          first_name: first name e
          user_num: 12
    ]]):strip())

    test:is_deeply(result_1_3.data, exp_result_1_3, '1_3')

    local result_1_4 = test_utils.show_trace(function()
        local variables_1_4 = {user_str = 'user_str_b'}
        return gql_query_1:execute(variables_1_4)
    end)

    local exp_result_1_4 = yaml.decode(([[
        ---
        user_collection:
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 1
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 2
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 3
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 4
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 5
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 6
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 7
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 8
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 9
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 10
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 11
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 12
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 13
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 14
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 15
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 16
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 17
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 18
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 19
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 20
    ]]):strip())

    test:is_deeply(result_1_4.data, exp_result_1_4, '1_4')

    -- }}}
    -- {{{ select top-level objects by a partial compound primary key plus
    -- filter (or maybe use fullscan)

    local result_1_5 = test_utils.show_trace(function()
        local variables_1_5 = {
            user_num = 12,
            first_name = 'non-existent'
        }
        return gql_query_1:execute(variables_1_5)
    end)

    local exp_result_1_5 = yaml.decode(([[
        ---
        user_collection: []
    ]]):strip())

    test:is_deeply(result_1_5.data, exp_result_1_5, '1_5')

    local result_1_6 = test_utils.show_trace(function()
        local variables_1_6 = {
            user_str = 'user_str_b',
            first_name = 'non-existent'
        }
        return gql_query_1:execute(variables_1_6)
    end)

    local exp_result_1_6 = yaml.decode(([[
        ---
        user_collection: []
    ]]):strip())

    test:is_deeply(result_1_6.data, exp_result_1_6, '1_6')

    -- }}}
    -- {{{ select objects by a connection by a full compound index

    local query_2 = [[
        query users($user_str: String, $user_num: Long, $description: String) {
            user_collection(user_str: $user_str, user_num: $user_num) {
                user_str
                user_num
                last_name
                first_name
                order_connection(description: $description) {
                    order_str
                    order_num
                    description
                }
            }
        }
    ]]

    local gql_query_2 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    local result_2_1 = test_utils.show_trace(function()
        local variables_2_1 = {user_str = 'user_str_b', user_num = 12}
        return gql_query_2:execute(variables_2_1)
    end)

    local exp_result_2_1 = yaml.decode(([[
        ---
        user_collection:
        - order_connection:
          - order_num: 1201
            order_str: order_str_b_1
            description: description b
          - order_num: 1210
            order_str: order_str_b_10
            description: description b
          - order_num: 1202
            order_str: order_str_b_2
            description: description b
          - order_num: 1203
            order_str: order_str_b_3
            description: description b
          - order_num: 1204
            order_str: order_str_b_4
            description: description b
          - order_num: 1205
            order_str: order_str_b_5
            description: description b
          - order_num: 1206
            order_str: order_str_b_6
            description: description b
          - order_num: 1207
            order_str: order_str_b_7
            description: description b
          - order_num: 1208
            order_str: order_str_b_8
            description: description b
          - order_num: 1209
            order_str: order_str_b_9
            description: description b
          user_str: user_str_b
          first_name: first name b
          user_num: 12
          last_name: last name b
    ]]):strip())

    test:is_deeply(result_2_1.data, exp_result_2_1, '2_1')

    -- }}}
    -- {{{ select objects by a connection by a full compound index plus filter

    local result_2_2 = test_utils.show_trace(function()
        local variables_2_2 = {
            user_str = 'user_str_b',
            user_num = 12,
            description = 'non-existent',
        }
        return gql_query_2:execute(variables_2_2)
    end)

    local exp_result_2_2 = yaml.decode(([[
        ---
        user_collection:
        - order_connection: []
          user_str: user_str_b
          first_name: first name b
          user_num: 12
          last_name: last name b
    ]]):strip())

    test:is_deeply(result_2_2.data, exp_result_2_2, '2_2')

    -- }}}
    -- {{{ select object by a connection by a partial compound index

    local query_3 = [[
        query users($user_str: String, $user_num: Long) {
            user_collection(user_str: $user_str, user_num: $user_num) {
                user_str
                user_num
                last_name
                first_name
                order_str_connection {
                    order_str
                    order_num
                    description
                }
            }
        }
    ]]

    local result_3 = test_utils.show_trace(function()
        local gql_query_3 = gql_wrapper:compile(query_3)
        local variables_3 = {user_str = 'user_str_b', user_num = 12}
        return gql_query_3:execute(variables_3)
    end)

    local exp_result_3 = yaml.decode(([[
        ---
        user_collection:
        - user_str: user_str_b
          first_name: first name b
          user_num: 12
          last_name: last name b
          order_str_connection:
          - order_num: 101
            order_str: order_str_b_1
            description: description b
          - order_num: 110
            order_str: order_str_b_10
            description: description b
          - order_num: 102
            order_str: order_str_b_2
            description: description b
          - order_num: 103
            order_str: order_str_b_3
            description: description b
          - order_num: 104
            order_str: order_str_b_4
            description: description b
          - order_num: 105
            order_str: order_str_b_5
            description: description b
          - order_num: 106
            order_str: order_str_b_6
            description: description b
          - order_num: 107
            order_str: order_str_b_7
            description: description b
          - order_num: 108
            order_str: order_str_b_8
            description: description b
          - order_num: 201
            order_str: order_str_b_1
            description: description b
          - order_num: 210
            order_str: order_str_b_10
            description: description b
          - order_num: 202
            order_str: order_str_b_2
            description: description b
          - order_num: 205
            order_str: order_str_b_5
            description: description b
          - order_num: 208
            order_str: order_str_b_8
            description: description b
          - order_num: 301
            order_str: order_str_b_1
            description: description b
          - order_num: 310
            order_str: order_str_b_10
            description: description b
          - order_num: 302
            order_str: order_str_b_2
            description: description b
          - order_num: 305
            order_str: order_str_b_5
            description: description b
          - order_num: 308
            order_str: order_str_b_8
            description: description b
          - order_num: 401
            order_str: order_str_b_1
            description: description b
          - order_num: 410
            order_str: order_str_b_10
            description: description b
          - order_num: 402
            order_str: order_str_b_2
            description: description b
          - order_num: 405
            order_str: order_str_b_5
            description: description b
          - order_num: 408
            order_str: order_str_b_8
            description: description b
          - order_num: 501
            order_str: order_str_b_1
            description: description b
          - order_num: 510
            order_str: order_str_b_10
            description: description b
          - order_num: 502
            order_str: order_str_b_2
            description: description b
          - order_num: 505
            order_str: order_str_b_5
            description: description b
          - order_num: 508
            order_str: order_str_b_8
            description: description b
          - order_num: 601
            order_str: order_str_b_1
            description: description b
          - order_num: 610
            order_str: order_str_b_10
            description: description b
          - order_num: 602
            order_str: order_str_b_2
            description: description b
          - order_num: 605
            order_str: order_str_b_5
            description: description b
          - order_num: 608
            order_str: order_str_b_8
            description: description b
          - order_num: 701
            order_str: order_str_b_1
            description: description b
          - order_num: 710
            order_str: order_str_b_10
            description: description b
          - order_num: 702
            order_str: order_str_b_2
            description: description b
          - order_num: 705
            order_str: order_str_b_5
            description: description b
          - order_num: 708
            order_str: order_str_b_8
            description: description b
          - order_num: 801
            order_str: order_str_b_1
            description: description b
          - order_num: 810
            order_str: order_str_b_10
            description: description b
          - order_num: 802
            order_str: order_str_b_2
            description: description b
          - order_num: 805
            order_str: order_str_b_5
            description: description b
          - order_num: 808
            order_str: order_str_b_8
            description: description b
          - order_num: 901
            order_str: order_str_b_1
            description: description b
          - order_num: 910
            order_str: order_str_b_10
            description: description b
          - order_num: 902
            order_str: order_str_b_2
            description: description b
          - order_num: 905
            order_str: order_str_b_5
            description: description b
          - order_num: 908
            order_str: order_str_b_8
            description: description b
          - order_num: 1001
            order_str: order_str_b_1
            description: description b
          - order_num: 1010
            order_str: order_str_b_10
            description: description b
          - order_num: 1002
            order_str: order_str_b_2
            description: description b
          - order_num: 1005
            order_str: order_str_b_5
            description: description b
          - order_num: 1008
            order_str: order_str_b_8
            description: description b
          - order_num: 1101
            order_str: order_str_b_1
            description: description b
          - order_num: 1110
            order_str: order_str_b_10
            description: description b
          - order_num: 1102
            order_str: order_str_b_2
            description: description b
          - order_num: 1105
            order_str: order_str_b_5
            description: description b
          - order_num: 1108
            order_str: order_str_b_8
            description: description b
          - order_num: 1201
            order_str: order_str_b_1
            description: description b
          - order_num: 1210
            order_str: order_str_b_10
            description: description b
          - order_num: 1202
            order_str: order_str_b_2
            description: description b
          - order_num: 1205
            order_str: order_str_b_5
            description: description b
          - order_num: 1208
            order_str: order_str_b_8
            description: description b
          - order_num: 1301
            order_str: order_str_b_1
            description: description b
          - order_num: 1310
            order_str: order_str_b_10
            description: description b
          - order_num: 1302
            order_str: order_str_b_2
            description: description b
          - order_num: 1305
            order_str: order_str_b_5
            description: description b
          - order_num: 1308
            order_str: order_str_b_8
            description: description b
          - order_num: 1401
            order_str: order_str_b_1
            description: description b
          - order_num: 1410
            order_str: order_str_b_10
            description: description b
          - order_num: 1402
            order_str: order_str_b_2
            description: description b
          - order_num: 1405
            order_str: order_str_b_5
            description: description b
          - order_num: 1408
            order_str: order_str_b_8
            description: description b
          - order_num: 1501
            order_str: order_str_b_1
            description: description b
          - order_num: 1510
            order_str: order_str_b_10
            description: description b
          - order_num: 1502
            order_str: order_str_b_2
            description: description b
          - order_num: 1505
            order_str: order_str_b_5
            description: description b
          - order_num: 1508
            order_str: order_str_b_8
            description: description b
          - order_num: 1601
            order_str: order_str_b_1
            description: description b
          - order_num: 1610
            order_str: order_str_b_10
            description: description b
          - order_num: 1602
            order_str: order_str_b_2
            description: description b
          - order_num: 1605
            order_str: order_str_b_5
            description: description b
          - order_num: 1608
            order_str: order_str_b_8
            description: description b
          - order_num: 1701
            order_str: order_str_b_1
            description: description b
          - order_num: 1710
            order_str: order_str_b_10
            description: description b
          - order_num: 1702
            order_str: order_str_b_2
            description: description b
          - order_num: 1705
            order_str: order_str_b_5
            description: description b
          - order_num: 1708
            order_str: order_str_b_8
            description: description b
          - order_num: 1801
            order_str: order_str_b_1
            description: description b
          - order_num: 1810
            order_str: order_str_b_10
            description: description b
          - order_num: 1802
            order_str: order_str_b_2
            description: description b
          - order_num: 1805
            order_str: order_str_b_5
            description: description b
          - order_num: 1808
            order_str: order_str_b_8
            description: description b
          - order_num: 1901
            order_str: order_str_b_1
            description: description b
          - order_num: 1910
            order_str: order_str_b_10
            description: description b
          - order_num: 1902
            order_str: order_str_b_2
            description: description b
          - order_num: 1905
            order_str: order_str_b_5
            description: description b
          - order_num: 1908
            order_str: order_str_b_8
            description: description b
          - order_num: 2001
            order_str: order_str_b_1
            description: description b
          - order_num: 2010
            order_str: order_str_b_10
            description: description b
          - order_num: 2002
            order_str: order_str_b_2
            description: description b
          - order_num: 2005
            order_str: order_str_b_5
            description: description b
          - order_num: 2008
            order_str: order_str_b_8
            description: description b
          - order_num: 109
            order_str: order_str_b_9
            description: description b
          - order_num: 203
            order_str: order_str_b_3
            description: description b
          - order_num: 204
            order_str: order_str_b_4
            description: description b
          - order_num: 206
            order_str: order_str_b_6
            description: description b
          - order_num: 207
            order_str: order_str_b_7
            description: description b
          - order_num: 209
            order_str: order_str_b_9
            description: description b
          - order_num: 303
            order_str: order_str_b_3
            description: description b
          - order_num: 304
            order_str: order_str_b_4
            description: description b
          - order_num: 306
            order_str: order_str_b_6
            description: description b
          - order_num: 307
            order_str: order_str_b_7
            description: description b
          - order_num: 309
            order_str: order_str_b_9
            description: description b
          - order_num: 403
            order_str: order_str_b_3
            description: description b
          - order_num: 404
            order_str: order_str_b_4
            description: description b
          - order_num: 406
            order_str: order_str_b_6
            description: description b
          - order_num: 407
            order_str: order_str_b_7
            description: description b
          - order_num: 409
            order_str: order_str_b_9
            description: description b
          - order_num: 503
            order_str: order_str_b_3
            description: description b
          - order_num: 504
            order_str: order_str_b_4
            description: description b
          - order_num: 506
            order_str: order_str_b_6
            description: description b
          - order_num: 507
            order_str: order_str_b_7
            description: description b
          - order_num: 509
            order_str: order_str_b_9
            description: description b
          - order_num: 603
            order_str: order_str_b_3
            description: description b
          - order_num: 604
            order_str: order_str_b_4
            description: description b
          - order_num: 606
            order_str: order_str_b_6
            description: description b
          - order_num: 607
            order_str: order_str_b_7
            description: description b
          - order_num: 609
            order_str: order_str_b_9
            description: description b
          - order_num: 703
            order_str: order_str_b_3
            description: description b
          - order_num: 704
            order_str: order_str_b_4
            description: description b
          - order_num: 706
            order_str: order_str_b_6
            description: description b
          - order_num: 707
            order_str: order_str_b_7
            description: description b
          - order_num: 709
            order_str: order_str_b_9
            description: description b
          - order_num: 803
            order_str: order_str_b_3
            description: description b
          - order_num: 804
            order_str: order_str_b_4
            description: description b
          - order_num: 806
            order_str: order_str_b_6
            description: description b
          - order_num: 807
            order_str: order_str_b_7
            description: description b
          - order_num: 809
            order_str: order_str_b_9
            description: description b
          - order_num: 903
            order_str: order_str_b_3
            description: description b
          - order_num: 904
            order_str: order_str_b_4
            description: description b
          - order_num: 906
            order_str: order_str_b_6
            description: description b
          - order_num: 907
            order_str: order_str_b_7
            description: description b
          - order_num: 909
            order_str: order_str_b_9
            description: description b
          - order_num: 1003
            order_str: order_str_b_3
            description: description b
          - order_num: 1004
            order_str: order_str_b_4
            description: description b
          - order_num: 1006
            order_str: order_str_b_6
            description: description b
          - order_num: 1007
            order_str: order_str_b_7
            description: description b
          - order_num: 1009
            order_str: order_str_b_9
            description: description b
          - order_num: 1103
            order_str: order_str_b_3
            description: description b
          - order_num: 1104
            order_str: order_str_b_4
            description: description b
          - order_num: 1106
            order_str: order_str_b_6
            description: description b
          - order_num: 1107
            order_str: order_str_b_7
            description: description b
          - order_num: 1109
            order_str: order_str_b_9
            description: description b
          - order_num: 1203
            order_str: order_str_b_3
            description: description b
          - order_num: 1204
            order_str: order_str_b_4
            description: description b
          - order_num: 1206
            order_str: order_str_b_6
            description: description b
          - order_num: 1207
            order_str: order_str_b_7
            description: description b
          - order_num: 1209
            order_str: order_str_b_9
            description: description b
          - order_num: 1303
            order_str: order_str_b_3
            description: description b
          - order_num: 1304
            order_str: order_str_b_4
            description: description b
          - order_num: 1306
            order_str: order_str_b_6
            description: description b
          - order_num: 1307
            order_str: order_str_b_7
            description: description b
          - order_num: 1309
            order_str: order_str_b_9
            description: description b
          - order_num: 1403
            order_str: order_str_b_3
            description: description b
          - order_num: 1404
            order_str: order_str_b_4
            description: description b
          - order_num: 1406
            order_str: order_str_b_6
            description: description b
          - order_num: 1407
            order_str: order_str_b_7
            description: description b
          - order_num: 1409
            order_str: order_str_b_9
            description: description b
          - order_num: 1503
            order_str: order_str_b_3
            description: description b
          - order_num: 1504
            order_str: order_str_b_4
            description: description b
          - order_num: 1506
            order_str: order_str_b_6
            description: description b
          - order_num: 1507
            order_str: order_str_b_7
            description: description b
          - order_num: 1509
            order_str: order_str_b_9
            description: description b
          - order_num: 1603
            order_str: order_str_b_3
            description: description b
          - order_num: 1604
            order_str: order_str_b_4
            description: description b
          - order_num: 1606
            order_str: order_str_b_6
            description: description b
          - order_num: 1607
            order_str: order_str_b_7
            description: description b
          - order_num: 1609
            order_str: order_str_b_9
            description: description b
          - order_num: 1703
            order_str: order_str_b_3
            description: description b
          - order_num: 1704
            order_str: order_str_b_4
            description: description b
          - order_num: 1706
            order_str: order_str_b_6
            description: description b
          - order_num: 1707
            order_str: order_str_b_7
            description: description b
          - order_num: 1709
            order_str: order_str_b_9
            description: description b
          - order_num: 1803
            order_str: order_str_b_3
            description: description b
          - order_num: 1804
            order_str: order_str_b_4
            description: description b
          - order_num: 1806
            order_str: order_str_b_6
            description: description b
          - order_num: 1807
            order_str: order_str_b_7
            description: description b
          - order_num: 1809
            order_str: order_str_b_9
            description: description b
          - order_num: 1903
            order_str: order_str_b_3
            description: description b
          - order_num: 1904
            order_str: order_str_b_4
            description: description b
          - order_num: 1906
            order_str: order_str_b_6
            description: description b
          - order_num: 1907
            order_str: order_str_b_7
            description: description b
          - order_num: 1909
            order_str: order_str_b_9
            description: description b
          - order_num: 2003
            order_str: order_str_b_3
            description: description b
          - order_num: 2004
            order_str: order_str_b_4
            description: description b
          - order_num: 2006
            order_str: order_str_b_6
            description: description b
          - order_num: 2007
            order_str: order_str_b_7
            description: description b
          - order_num: 2009
            order_str: order_str_b_9
            description: description b
    ]]):strip())

    -- XXX: gh-40: sorting is different over space and space anf even over
    -- different shard configurations
    local function comparator(a, b)
        return a.order_num < b.order_num
    end
    table.sort(result_3.data.user_collection[1].order_str_connection,
        comparator)
    table.sort(exp_result_3.user_collection[1].order_str_connection, comparator)
    test:is_deeply(result_3.data, exp_result_3, '3')

    -- }}}
    -- {{{ offset on top-level by a full compound primary key

    local query_4 = [[
        query users($limit: Int, $offset: user_collection_offset) {
            user_collection(limit: $limit, offset: $offset) {
                user_str
                user_num
                last_name
                first_name
            }
        }
    ]]

    local gql_query_4 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_4)
    end)

    local result_4_1 = test_utils.show_trace(function()
        local variables_4_1 = {
            limit = 10,
            offset = {
                user_str = 'user_str_b',
                user_num = 12,
            }
        }
        return gql_query_4:execute(variables_4_1)
    end)

    local exp_result_4_1 = yaml.decode(([[
        ---
        user_collection:
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 13
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 14
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 15
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 16
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 17
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 18
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 19
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 20
        - last_name: last name c
          user_str: user_str_c
          first_name: first name c
          user_num: 1
        - last_name: last name c
          user_str: user_str_c
          first_name: first name c
          user_num: 2
    ]]):strip())

    test:is_deeply(result_4_1.data, exp_result_4_1, '4_1')

    -- }}}
    -- {{{ offset on top-level by a partial compound primary key (expected to
    -- fail)

    local variables_4_2 = {
        limit = 10,
        offset = {
            user_str = 'user_str_b',
        }
    }
    local result = gql_query_4:execute(variables_4_2)
    local err = result.errors[1].message
    local exp_err = 'Variable "offset.user_num" expected to be non-null'
    test:is(err, exp_err, '4_2')

    -- }}}
    -- {{{ offset when using a connection by a full compound primary key

    local query_5 = [[
        query users($user_str: String, $user_num: Long,
                $limit: Int, $offset: order_collection_offset) {
            user_collection(user_str: $user_str, user_num: $user_num) {
                user_str
                user_num
                last_name
                first_name
                order_connection(limit: $limit, offset: $offset) {
                    order_str
                    order_num
                    description
                }
            }
        }
    ]]

    local gql_query_5 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_5)
    end)

    local result_5_1 = test_utils.show_trace(function()
        local variables_5_1 = {
            user_str = 'user_str_b',
            user_num = 12,
            limit = 4,
            offset = {
                order_str = 'order_str_b_2',
                order_num = 1202,
            }
        }
        return gql_query_5:execute(variables_5_1)
    end)

    local exp_result_5_1 = yaml.decode(([[
        ---
        user_collection:
        - order_connection:
          - order_num: 1203
            order_str: order_str_b_3
            description: description b
          - order_num: 1204
            order_str: order_str_b_4
            description: description b
          - order_num: 1205
            order_str: order_str_b_5
            description: description b
          - order_num: 1206
            order_str: order_str_b_6
            description: description b
          user_str: user_str_b
          first_name: first name b
          user_num: 12
          last_name: last name b
    ]]):strip())

    test:is_deeply(result_5_1.data, exp_result_5_1, '5_1')

    -- }}}
    -- {{{ offset when using a connection by a partial compound primary key
    -- (expected to fail)

    local variables_5_2 = {
        user_str = 'user_str_b',
        user_num = 12,
        limit = 4,
        offset = {
            order_str = 'order_str_b_2',
        }
    }
    local result = gql_query_5:execute(variables_5_2)
    local err = result.errors[1].message
    local exp_err = 'Variable "offset.order_num" expected to be non-null'
    test:is(err, exp_err, '5_2')

    -- }}}
    -- {{{ compound offset argument constructed from separate variables
    -- (top-level collection, full primary key)

    local query_6 = [[
        query users($limit: Int, $user_str: String!, $user_num: Long!) {
            user_collection(limit: $limit, offset: {user_str: $user_str,
                    user_num: $user_num}) {
                user_str
                user_num
                last_name
                first_name
            }
        }
    ]]

    local result_6 = test_utils.show_trace(function()
        local gql_query_6 = gql_wrapper:compile(query_6)
        local variables_6 = {
            limit = 10,
            user_str = 'user_str_b',
            user_num = 12,
        }
        return gql_query_6:execute(variables_6)
    end)

    local exp_result_6 = yaml.decode(([[
        ---
        user_collection:
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 13
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 14
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 15
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 16
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 17
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 18
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 19
        - last_name: last name b
          user_str: user_str_b
          first_name: first name b
          user_num: 20
        - last_name: last name c
          user_str: user_str_c
          first_name: first name c
          user_num: 1
        - last_name: last name c
          user_str: user_str_c
          first_name: first name c
          user_num: 2
    ]]):strip())

    test:is_deeply(result_6.data, exp_result_6, '6')

    -- }}}

    assert(test:check(), 'check plan')
end

return compound_index_testdata

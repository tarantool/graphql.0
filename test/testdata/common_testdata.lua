local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local common_testdata = {}

-- needed to compare a dump with floats/doubles, because, say,
-- `tonumber(tostring(1/3)) == 1/3` is `false`
local function deeply_number_tostring(t)
    if type(t) == 'table' then
        local res = {}
        for k, v in pairs(t) do
            res[k] = deeply_number_tostring(v)
        end
        return res
    elseif type(t) == 'number' then
        return tostring(t)
    else
        return table.deepcopy(t)
    end
end

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
                { "name": "description", "type": "string" },
                { "name": "price", "type": "double" },
                { "name": "discount", "type": "float" },
                { "name": "in_stock", "type": "boolean", "default": true }
            ]
        },
        "order_metainfo": {
            "type": "record",
            "name": "order_metainfo",
            "fields": [
                { "name": "metainfo", "type": "string" },
                { "name": "order_metainfo_id", "type": "string" },
                { "name": "order_id", "type": "string" },
                { "name": "store", "type": {
                    "type": "record",
                    "name": "store",
                    "fields": [
                        { "name": "name", "type": "string" },
                        { "name": "address", "type": {
                            "type": "record",
                            "name": "address",
                            "fields": [
                                { "name": "street", "type": "string" },
                                { "name": "city", "type": "string" },
                                { "name": "state", "type": "string" },
                                { "name": "zip", "type": "string" }
                            ]
                        }},
                        { "name": "second_address", "type": "address" },
                        { "name": "external_id", "type": ["int", "string"]},
                        { "name": "tags", "type": {
                            "type": "array",
                            "items": "string"
                        }},
                        { "name": "parametrized_tags", "type": {
                            "type": "map",
                            "values": "string"
                        }}
                    ]
                }}
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
                },
                {
                    "type": "1:1",
                    "name": "order_metainfo_connection",
                    "destination_collection":  "order_metainfo_collection",
                    "parts": [
                        {
                            "source_field": "order_id",
                            "destination_field": "order_id"
                        }
                    ],
                    "index_name": "order_id_index"
                }
            ]
        },
        "order_metainfo_collection": {
            "schema_name": "order_metainfo",
            "connections": []
        }
    }]])

    local service_fields = {
        user = {
            {name = 'expires_on', type = 'long', default = 0},
        },
        order = {},
        order_metainfo = {},
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
        order_metainfo_collection = {
            order_metainfo_id_index = {
                service_fields = {},
                fields = {'order_metainfo_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            order_id_index = {
                service_fields = {},
                fields = {'order_id'},
                index_type = 'tree',
                unique = true,
                primary = false,
            }
        }
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

    -- order_metainfo_collection fields
    local M_ORDER_METAINFO_ID_FN = 2
    local M_ORDER_ID_FN = 3

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
        box.schema.create_space('order_metainfo_collection')
        box.space.order_metainfo_collection:create_index(
            'order_metainfo_id_index',
            {type = 'tree', parts = {
                M_ORDER_METAINFO_ID_FN, 'string'
            }}
        )
        box.space.order_metainfo_collection:create_index('order_id_index',
            {type = 'tree', parts = {
                M_ORDER_ID_FN, 'string'
            }}
        )
    end)
end

function common_testdata.fill_test_data(virtbox, meta)
    test_utils.replace_object(virtbox, meta, 'user_collection', {
        user_id = 'user_id_1',
        first_name = 'Ivan',
        middle_name = 'Ivanovich',
        last_name = 'Ivanov',
    }, {
        1827767717,
    })
    test_utils.replace_object(virtbox, meta, 'user_collection', {
        user_id = 'user_id_2',
        first_name = 'Vasiliy',
        middle_name = box.NULL,
        last_name = 'Pupkin',
    }, {
        1827767717,
    })

    test_utils.replace_object(virtbox, meta, 'order_collection', {
        order_id = 'order_id_1',
        user_id = 'user_id_1',
        description = 'first order of Ivan',
        price = 0,
        discount = 0,
        in_stock = true,
    })
    test_utils.replace_object(virtbox, meta, 'order_collection', {
        order_id = 'order_id_2',
        user_id = 'user_id_1',
        description = 'second order of Ivan',
        price = 0,
        discount = 0,
        in_stock = false,
    })
    test_utils.replace_object(virtbox, meta, 'order_collection', {
        order_id = 'order_id_3',
        user_id = 'user_id_2',
        description = 'first order of Vasiliy',
        price = 0,
        discount = 0,
        in_stock = true,
    })

    for i = 3, 100 do
        local s = tostring(i)
        test_utils.replace_object(virtbox, meta, 'user_collection', {
            user_id = 'user_id_' .. s,
            first_name = 'first name ' .. s,
            middle_name = box.NULL,
            last_name = 'last name ' .. s,
        }, {
            1827767717,
        })
        for j = (4 + (i - 3) * 40), (4 + (i - 2) * 40) - 1 do
            local t = tostring(j)
            test_utils.replace_object(virtbox, meta, 'order_collection', {
                order_id = 'order_id_' .. t,
                user_id = 'user_id_' .. s,
                description = 'order of user ' .. s,
                price = i + j / 3,
                discount = i + j / 3,
                in_stock = j % 2 == 1,
            })
        end
    end

    test_utils.replace_object(virtbox, meta, 'user_collection', {
        user_id = 'user_id_101',
        first_name = 'Иван',
        middle_name = 'Иванович',
        last_name = 'Иванов',
    }, {
        1827767717,
    })
    test_utils.replace_object(virtbox, meta, 'order_collection', {
        order_id = 'order_id_3924',
        user_id = 'user_id_101',
        description = 'Покупка 3924',
        price = 0,
        discount = 0,
        in_stock = true,
    })

    for i = 1, 3924 do
        local s = tostring(i)
        test_utils.replace_object(virtbox, meta, 'order_metainfo_collection', {
            metainfo = 'order metainfo ' .. s,
            order_metainfo_id = 'order_metainfo_id_' .. s,
            order_id = 'order_id_' .. s,
            store = {
                name = 'store ' .. s,
                address = {
                    street = 'street ' .. s,
                    city = 'city ' .. s,
                    state = 'state ' .. s,
                    zip = 'zip ' .. s,
                },
                second_address = {
                    street = 'second street ' .. s,
                    city = 'second city ' .. s,
                    state = 'second state ' .. s,
                    zip = 'second zip ' .. s,
                },
                external_id = i % 2 == 1 and {int = i} or
                    {string = 'eid_' .. s},
                tags = {'fast', 'new'},
                parametrized_tags = {
                    size = 'medium',
                    since = '2018-01-01'
                },
            }
        })
    end
end

function common_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
    box.space.order_collection:drop()
    box.space.order_metainfo_collection:drop()
end

function common_testdata.run_queries(gql_wrapper)
    local test = tap.test('common')
    test:plan(24)

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

    local variables_1 = {order_id = 'order_id_1'}

    test_utils.show_trace(function()
        local gql_query_1 = gql_wrapper:compile(query_1)
        local result = gql_query_1:execute(variables_1)
        test:is_deeply(result, exp_result_1, '1')
    end)

    local query_1n = [[
        query($order_id: String) {
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

    test_utils.show_trace(function()
        local gql_query_1n = gql_wrapper:compile(query_1n)
        local result = gql_query_1n:execute(variables_1)
        test:is_deeply(result, exp_result_1, '1n')
    end)

    local query_1inn = [[
        {
            order_collection(order_id: "order_id_1") {
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

    test_utils.show_trace(function()
        local gql_query_1inn = gql_wrapper:compile(query_1inn)
        local result = gql_query_1inn:execute({})
        test:is_deeply(result, exp_result_1, '1inn')
    end)

    local query_1tn = [[
        query get_order {
            order_collection(order_id: "order_id_1") {
                order_id
                description
            }
        }
        query {
            order_collection(order_id: "order_id_1") {
                order_id
                description
            }
        }
    ]]

    local err_exp = 'Cannot have more than one operation when using ' ..
        'anonymous operations'
    local ok, err = pcall(gql_wrapper.compile, gql_wrapper, query_1tn)
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        'unnamed query should be a single one')

    local query_1t = [[
        query user_by_order {
            order_collection(order_id: "order_id_1") {
                order_id
                description
                user_connection {
                    user_id
                    last_name
                    first_name
                }
            }
        }
        query get_order {
            order_collection(order_id: "order_id_1") {
                order_id
                description
            }
        }
    ]]

    local gql_query_1t = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1t)
    end)

    local err_exp = 'Operation name must be specified if more than one ' ..
        'operation exists.'
    local ok, err = pcall(gql_query_1t.execute, gql_query_1t, {})
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        'non-determined query name should give an error')

    local err_exp = 'Unknown operation "non_existent_operation"'
    local ok, err = pcall(gql_query_1t.execute, gql_query_1t, {},
        'non_existent_operation')
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        'wrong operation name should give an error')

    test_utils.show_trace(function()
        local result = gql_query_1t:execute({}, 'user_by_order')
        test:is_deeply(result, exp_result_1, 'execute an operation by name')
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

    local gql_query_2 = test_utils.show_trace(function()
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

    test_utils.show_trace(function()
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

    test_utils.show_trace(function()
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

    test_utils.show_trace(function()
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

    test_utils.show_trace(function()
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
    test_utils.show_trace(function()
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

    test_utils.show_trace(function()
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

    local gql_query_4 = test_utils.show_trace(function()
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
    test_utils.show_trace(function()
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
    test_utils.show_trace(function()
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

    local gql_query_5 = test_utils.show_trace(function()
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
    test_utils.show_trace(function()
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
    test_utils.show_trace(function()
        local variables_5_2 = {
            first_name = 'non-existent user',
            description = 'first order of Ivan',
        }
        local result = gql_query_5:execute(variables_5_2)
        test:is_deeply(result, exp_result_5_2, '5_2')
    end)
    ]=]--

    -- {{{ float, double

    local query_6 = [[
        query order($limit: Int, $offset: String, $in_stock: Boolean) {
            order_collection(limit: $limit, offset: $offset,
                    in_stock: $in_stock) {
                order_id
                user_id
                price
                discount
                in_stock
            }
        }
    ]]

    local query_6_i_true = [[
        query order($limit: Int, $offset: String) {
            order_collection(limit: $limit, offset: $offset,
                    in_stock: true) {
                order_id
                user_id
                price
                discount
                in_stock
            }
        }
    ]]

    local query_6_i_false = [[
        query order($limit: Int, $offset: String) {
            order_collection(limit: $limit, offset: $offset,
                    in_stock: false) {
                order_id
                user_id
                price
                discount
                in_stock
            }
        }
    ]]

    local gql_query_6 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_6)
    end)
    local gql_query_6_i_true = test_utils.show_trace(function()
        return gql_wrapper:compile(query_6_i_true)
    end)
    local gql_query_6_i_false = test_utils.show_trace(function()
        return gql_wrapper:compile(query_6_i_false)
    end)

    local exp_result_6_1 = yaml.decode(([[
        ---
        order_collection:
        - price: 0
          in_stock: true
          user_id: user_id_1
          order_id: order_id_1
          discount: 0
        - price: 6.3333333333333
          in_stock: false
          user_id: user_id_3
          order_id: order_id_10
          discount: 6.3333334922791
        - price: 38.333333333333
          in_stock: false
          user_id: user_id_5
          order_id: order_id_100
          discount: 38.333332061768
        - price: 360.33333333333
          in_stock: false
          user_id: user_id_27
          order_id: order_id_1000
          discount: 360.33334350586
        - price: 360.66666666667
          in_stock: true
          user_id: user_id_27
          order_id: order_id_1001
          discount: 360.66665649414
        - price: 361
          in_stock: false
          user_id: user_id_27
          order_id: order_id_1002
          discount: 361
        - price: 361.33333333333
          in_stock: true
          user_id: user_id_27
          order_id: order_id_1003
          discount: 361.33334350586
        - price: 362.66666666667
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1004
          discount: 362.66665649414
        - price: 363
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1005
          discount: 363
        - price: 363.33333333333
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1006
          discount: 363.33334350586
    ]]):strip())

    test_utils.show_trace(function()
        local variables_6_1 = {limit = 10}
        local result = gql_query_6:execute(variables_6_1)
        local exp_result_6_1 = deeply_number_tostring(exp_result_6_1)
        local result = deeply_number_tostring(result)
        test:is_deeply(result, exp_result_6_1, '6_1')
    end)

    local exp_result_6_2 = yaml.decode(([[
        ---
        order_collection:
        - price: 0
          in_stock: true
          user_id: user_id_1
          order_id: order_id_1
          discount: 0
        - price: 360.66666666667
          in_stock: true
          user_id: user_id_27
          order_id: order_id_1001
          discount: 360.66665649414
        - price: 361.33333333333
          in_stock: true
          user_id: user_id_27
          order_id: order_id_1003
          discount: 361.33334350586
        - price: 363
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1005
          discount: 363
        - price: 363.66666666667
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1007
          discount: 363.66665649414
        - price: 364.33333333333
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1009
          discount: 364.33334350586
        - price: 38.666666666667
          in_stock: true
          user_id: user_id_5
          order_id: order_id_101
          discount: 38.666667938232
        - price: 365
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1011
          discount: 365
        - price: 365.66666666667
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1013
          discount: 365.66665649414
        - price: 366.33333333333
          in_stock: true
          user_id: user_id_28
          order_id: order_id_1015
          discount: 366.33334350586
    ]]):strip())

    test_utils.show_trace(function()
        local exp_result_6_2 = deeply_number_tostring(exp_result_6_2)

        local variables_6_2 = {limit = 10, in_stock = true}
        local result = gql_query_6:execute(variables_6_2)
        local result = deeply_number_tostring(result)
        test:is_deeply(result, exp_result_6_2, '6_2')

        local variables_6_2 = {limit = 10}
        local result = gql_query_6_i_true:execute(variables_6_2)
        local result = deeply_number_tostring(result)
        test:is_deeply(result, exp_result_6_2, '6_2')
    end)

    local exp_result_6_3 = yaml.decode(([[
        ---
        order_collection:
        - price: 6.3333333333333
          in_stock: false
          user_id: user_id_3
          order_id: order_id_10
          discount: 6.3333334922791
        - price: 38.333333333333
          in_stock: false
          user_id: user_id_5
          order_id: order_id_100
          discount: 38.333332061768
        - price: 360.33333333333
          in_stock: false
          user_id: user_id_27
          order_id: order_id_1000
          discount: 360.33334350586
        - price: 361
          in_stock: false
          user_id: user_id_27
          order_id: order_id_1002
          discount: 361
        - price: 362.66666666667
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1004
          discount: 362.66665649414
        - price: 363.33333333333
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1006
          discount: 363.33334350586
        - price: 364
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1008
          discount: 364
        - price: 364.66666666667
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1010
          discount: 364.66665649414
        - price: 365.33333333333
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1012
          discount: 365.33334350586
        - price: 366
          in_stock: false
          user_id: user_id_28
          order_id: order_id_1014
          discount: 366
    ]]):strip())

    test_utils.show_trace(function()
        local exp_result_6_3 = deeply_number_tostring(exp_result_6_3)

        local variables_6_3 = {limit = 10, in_stock = false}
        local result = gql_query_6:execute(variables_6_3)
        local result = deeply_number_tostring(result)
        test:is_deeply(result, exp_result_6_3, '6_3')

        local variables_6_3 = {limit = 10}
        local result = gql_query_6_i_false:execute(variables_6_3)
        local result = deeply_number_tostring(result)
        test:is_deeply(result, exp_result_6_3, '6_3')
    end)

    -- should fail
    local query_7 = [[
        query order {
            order_collection(price: 10.0) {
                order_id
                user_id
                price
                discount
                in_stock
            }
        }
    ]]

    local exp_result_7 = yaml.decode(([[
        ---
        ok: false
        err: Non-existent argument "price"
    ]]):strip())

    local ok, err = pcall(function()
        return gql_wrapper:compile(query_7)
    end)

    local result = {ok = ok, err = test_utils.strip_error(err)}
    test:is_deeply(result, exp_result_7, '7')

    -- should fail
    local query_8 = [[
        query order($price: Float) {
            order_collection(price: $price) {
                order_id
                user_id
                price
                discount
                in_stock
            }
        }
    ]]

    local exp_result_8 = yaml.decode(([[
        ---
        ok: false
        err: Non-existent argument "price"
    ]]):strip())

    local ok, err = pcall(function()
        return gql_wrapper:compile(query_8)
    end)

    local result = {ok = ok, err = test_utils.strip_error(err)}
    test:is_deeply(result, exp_result_8, '8')

    -- should fail
    local query_9 = [[
        query order($price: Double) {
            order_collection(price: $price) {
                order_id
                user_id
                price
                discount
                in_stock
            }
        }
    ]]

    local exp_result_9 = yaml.decode(([[
        ---
        ok: false
        err: Non-existent argument "price"
    ]]):strip())

    local ok, err = pcall(function()
        return gql_wrapper:compile(query_9)
    end)

    local result = {ok = ok, err = test_utils.strip_error(err)}
    test:is_deeply(result, exp_result_9, '9')

    -- }}}

    assert(test:check(), 'check plan')
end

return common_testdata

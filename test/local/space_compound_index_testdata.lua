local json = require('json')

local space_compound_index_testdata = {}

-- schemas and meta-information
-- ----------------------------

function space_compound_index_testdata.get_test_metadata()
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

function space_compound_index_testdata.init_spaces()
    -- user_collection fields
    local U_USER_STR_FN = 1
    local U_USER_NUM_FN = 2

    -- order_collection fields
    local O_ORDER_STR_FN = 1
    local O_ORDER_NUM_FN = 2
    local O_USER_STR_FN = 3
    local O_USER_NUM_FN = 4

    box.cfg{background = false}
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

function space_compound_index_testdata.fill_test_data()
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
            box.space.user_collection:replace(
                {'user_str_' .. s, i, 'first name ' .. s, 'last name ' .. s})
            for k = 1, 10 do
                box.space.order_collection:replace(
                    {'order_str_' .. s .. '_' .. tostring(k), i * 100 + k,
                    'user_str_' .. s, i, 'description ' .. s})
            end
        end
    end
end

function space_compound_index_testdata.drop()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
    box.space.order_collection:drop()
end

return space_compound_index_testdata

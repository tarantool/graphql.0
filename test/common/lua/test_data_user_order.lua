local json = require('json')

local testdata = {}

testdata.meta = {
    schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                { "name": "id", "type": "int" },
                { "name": "first_name", "type": "string" },
                { "name": "last_name", "type": "string" }
            ]
        },
        "order": {
            "type": "record",
            "name": "order",
            "fields": [
                { "name": "id", "type": "int" },
                { "name": "user_id", "type": "int" },
                { "name": "description", "type": "string" }
            ]
        }
    }]]),
    collections = json.decode([[{
        "user_collection": {
            "schema_name": "user",
            "connections": [
                {
                    "type": "1:N",
                    "name": "order_connection",
                    "destination_collection":  "order_collection",
                    "parts": [
                        { "source_field": "id", "destination_field": "user_id" }
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
                        { "source_field": "user_id", "destination_field": "id" }
                    ],
                    "index_name": "user_id_index"
                }
            ]
        }
    }]]),
    service_fields = {
        user = {
            {name = 'created', type = 'long', default = 0},
        },
        order = {},
    },
    indexes = {
        user_collection = {
            user_id_index = {
                service_fields = {},
                fields = {'id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
        order_collection = {
            order_id_index = {
                service_fields = {},
                fields = {'id'},
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
}


function testdata.init_spaces()
    -- user_collection fields
    local U_USER_ID_FN = 2

    -- order_collection fields
    local O_ORDER_ID_FN = 1
    local O_USER_ID_FN = 2

    box.schema.create_space('user_collection')
    box.space.user_collection:create_index('user_id_index',
        {type = 'tree', unique = true, parts = {
            U_USER_ID_FN, 'unsigned'
        }})

    box.schema.create_space('order_collection')
    box.space.order_collection:create_index('order_id_index',
        {type = 'tree', parts = {
            O_ORDER_ID_FN, 'unsigned'
        }})
    box.space.order_collection:create_index('user_id_index',
        {type = 'tree', unique = false, parts = {
            O_USER_ID_FN, 'unsigned'
        }})
end

function testdata.drop_spaces()
    box.space.user_collection:drop()
    box.space.order_collection:drop()
end


function testdata.fill_test_data(virtbox)
    local order_id = 1
    for i = 1, 15 do
        virtbox.user_collection:replace(
            {1827767717, i, 'user fn ' .. i, 'user ln ' .. i})
        -- Each user has N orders, where `N = user id`
        for j = 1, i do
            virtbox.order_collection:replace(
                {order_id, i, 'order of user ' .. i})
            order_id = order_id + 1
        end
    end
end

return testdata

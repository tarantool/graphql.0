local json = require('json')

-- This module helps to write tests providing simple shop database.
--
-- Data layout
--
-- models:
--  - user: Users of the shop.
--    - id
--    - first_name
--    - last_name
--  - order: Orders (shopping carts) of users. One user can has many orders.
--    - id
--    - user_id
--    - description
--  - item: Goods of the shop.
--    - id
--    - name
--    - description
--    - price
--  - order_item: N:N connection emulation. That is how one order can have many
--    itema and one item can be in different orders.
--    - order_id
--    - item_id

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
        },
        "item": {
            "type": "record",
            "name": "item",
            "fields": [
                { "name": "id", "type": "int" },
                { "name": "description", "type": "string" },
                { "name": "name", "type": "string" },
                { "name": "price", "type": "string" }
            ]
        },
        "order_item": {
            "type": "record",
            "name": "order_item",
            "fields": [
                { "name": "item_id", "type": "int" },
                { "name": "order_id", "type": "int" }
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
                        {
                            "source_field": "id",
                            "destination_field": "user_id"
                        }
                    ],
                    "index_name": "user_id_index"
                }
            ]
        },
        "order_collection": {
            "schema_name": "order",
            "connections": [
                {
                    "type": "1:N",
                    "name": "order__order_item",
                    "destination_collection":  "order_item_collection",
                    "parts": [
                        {
                            "source_field": "id",
                            "destination_field": "order_id"
                        }
                    ],
                    "index_name": "order_id_item_id_index"
                },
                {
                    "type": "1:1",
                    "name": "user_connection",
                    "destination_collection":  "user_collection",
                    "parts": [
                        {
                            "source_field": "user_id",
                            "destination_field": "id"
                        }
                    ],
                    "index_name": "user_id_index"
                }
            ]
        },
        "item_collection": {
            "schema_name": "item",
            "connections": [
            ]
        },
        "order_item_collection": {
            "schema_name": "order_item",
            "connections": [
                {
                    "type": "1:1",
                    "name": "order_item__order",
                    "destination_collection":  "order_collection",
                    "parts": [
                        {
                            "source_field": "order_id",
                            "destination_field": "id"
                        }
                    ],
                    "index_name": "order_id_index"
                },
                {
                    "type": "1:1",
                    "name": "order_item__item",
                    "destination_collection":  "item_collection",
                    "parts": [
                        {
                            "source_field": "item_id",
                            "destination_field": "id"
                        }
                    ],
                    "index_name": "item_id_index"
                }
            ]
        }
    }]]),
    service_fields = {
        user = {
            {name = 'created', type = 'long', default = 0},
        },
        order = {},
        item = {},
        order_item = {},
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
        item_collection = {
            item_id_index = {
                service_fields = {},
                fields = {'id'},
                index_type = 'tree',
                unique = true,
                primary = true
            }
        },
        order_item_collection = {
            order_id_item_id_index = {
                service_fields = {},
                fields = {'order_id', 'item_id'},
                index_type = 'tree',
                unique = true,
                primary = true
            }
        }
    }
}


function testdata.init_spaces(_, SHARD_EXTRA_FIELDS)
    SHARD_EXTRA_FIELDS = SHARD_EXTRA_FIELDS or 0
    -- user_collection fields
    local U_USER_ID_FN = 2 + SHARD_EXTRA_FIELDS

    -- order_collection fields
    local O_ORDER_ID_FN = 1 + SHARD_EXTRA_FIELDS
    local O_USER_ID_FN = 2 + SHARD_EXTRA_FIELDS

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
    -- item_collection fields
    local I_ITEM_ID_FN = 1
    box.schema.create_space('item_collection')
    box.space.item_collection:create_index('item_id_index',
        {type = 'tree', unique = true, parts = {
            I_ITEM_ID_FN, 'unsigned'
        }})
    -- order_item_collection fields
    local OI_ORDER_ID_FN = 1
    local OI_USER_ID_FN = 2
    box.schema.create_space('order_item_collection')
    box.space.order_item_collection:create_index('order_id_item_id_index',
        {type = 'tree', unique = true, parts = {
            OI_ORDER_ID_FN, 'unsigned', OI_USER_ID_FN, 'unsigned'
        }})
end

function testdata.drop_spaces()
    box.space.user_collection:drop()
    box.space.order_collection:drop()
    box.space.item_collection:drop()
    box.space.order_item_collection:drop()
end

local items

function testdata.fill_test_data(virtbox)
    local order_id = 1
    local item_id_max = #items
    for _, item in ipairs(items) do
       virtbox.item_collection:replace({
           item.id, item.description, item.name, item.price
       })
    end
    local order_item_cnt = 0
    for user_id = 1, 15 do
        virtbox.user_collection:replace_object(
            {
                id = user_id,
                first_name = 'user fn ' .. user_id,
                last_name = 'user ln ' .. user_id,
            }, { 1827767717 })
        -- Each user has N orders, where `N = user id`
        for i = 1, user_id do
            virtbox.order_collection:replace_object({
                id = order_id,
                user_id = user_id,
                description = 'order of user ' .. user_id,
            })
            order_id = order_id + 1
            local items_cnt = 3
            for k = 1, items_cnt do
                order_item_cnt = order_item_cnt + 1
                local item_id = order_item_cnt % item_id_max + 1
                virtbox.order_item_collection:replace_object({
                    item_id = order_id,
                    order_id = item_id,
                })
            end
        end
    end
end

items = {
    {
        id = 1,
        description = "rhoncus. Nullam velit dui, semper",
        name = "Salt",
        price = "7.51"
    },
    {
        id = 2,
        description = "sit",
        name = "butter",
        price = "3.96"
    },
    {
        id = 3,
        description = "non,",
        name = "onion",
        price = "2.83"
    },
    {
        id = 4,
        description = "mauris",
        name = "milk",
        price = "3.53"
    },
    {
        id = 5,
        description = "Suspendisse tristique neque venenatis",
        name = "Sausage",
        price = "1.84"
    },
    {
        id = 6,
        description = "eget, dictum",
        name = "Paper",
        price = "7.83"
    },
    {
        id = 7,
        description = "lectus quis massa. Mauris",
        name = "Freezer",
        price = "5.47"
    },
    {
        id = 8,
        description = "ac",
        name = "Stone",
        price = "8.29"
    },
    {
        id = 9,
        description = "natoque penatibus et magnis dis",
        name = "Silk",
        price = "1.60"
    },
    {
        id = 10,
        description = "adipiscing",
        name = "Leather",
        price = "0.40"
    },
    {
        id = 11,
        description = "lobortis ultrices. Vivamus rhoncus.",
        name = "Money",
        price = "9.74"
    },
    {
        id = 12,
        description = "montes, nascetur ridiculus",
        name = "Tree",
        price = "8.52"
    },
    {
        id = 13,
        description = "In at pede. Cras vulputate",
        name = "Garbage",
        price = "1.88"
    },
    {
        id = 14,
        description = "dolor quam, elementum at,",
        name = "Table",
        price = "2.91"
    },
    {
        id = 15,
        description = "Donec dignissim",
        name = "Wire",
        price = "6.04"
    },
    {
        id = 16,
        description = "turpis nec mauris blandit",
        name = "Cup",
        price = "8.05"
    },
    {
        id = 17,
        description = "ornare placerat, orci",
        name = "Blade",
        price = "2.58"
    },
    {
        id = 18,
        description = "arcu. Sed",
        name = "Tea",
        price = "0.38"
    },
    {
        id = 19,
        description = "tempus risus. Donec egestas. Duis",
        name = "Sveater",
        price = "8.66"
    },
    {
        id = 20,
        description = "Quisque libero lacus, varius",
        name = "Keyboard",
        price = "3.74"
    },
    {
        id = 21,
        description = "faucibus orci luctus et ultrices",
        name = "Shoes",
        price = "2.21"
    },
    {
        id = 22,
        description = "rhoncus. Nullam velit",
        name = "Lemon",
        price = "3.70"
    },
    {
        id = 23,
        description = "justo sit amet",
        name = "Orange",
        price = "9.27"
    },
    {
        id = 24,
        description = "porttitor tellus non magna.",
        name = "Pen",
        price = "3.41"
    },
    {
        id = 25,
        description = "Suspendisse dui. Fusce diam",
        name = "Screen",
        price = "1.22"
    },
    {
        id = 26,
        description = "eleifend vitae, erat. Vivamus nisi.",
        name = "Glass",
        price = "8.59"
    },
    {
        id = 27,
        description = "tincidunt, nunc",
        name = "Book",
        price = "4.24"
    },
    {
        id = 28,
        description = "orci luctus et ultrices posuere",
        name = "Mouse",
        price = "7.73"
    },
    {
        id = 29,
        description = "in,",
        name = "Doll",
        price = "2.13"
    },
    {
        id = 30,
        description = "lobortis ultrices. Vivamus rhoncus.",
        name = "Socks",
        price = "0.91"
    }
}

return testdata

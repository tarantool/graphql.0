local json = require('json')
local avro_schema = require('avro_schema')
local lhash = require('vshard.hash')

local  BUCKET_CNT = 3000
local function get_bucket_id(...)
    local key = {...}
    return lhash.key_hash(key) % BUCKET_CNT + 1
end

local test_metadata = {
    schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                { "name": "bucket_id", "type": "int" },
                { "name": "user_id", "type": "string" },
                { "name": "first_name", "type": "string" },
                { "name": "middle_name", "type": "string" },
                { "name": "last_name", "type": "string" }
            ]
        },
        "order": {
            "type": "record",
            "name": "order",
            "fields": [
                { "name": "bucket_id", "type": "int" },
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
                { "name": "bucket_id", "type": "int" },
                { "name": "metainfo", "type": "string" },
                { "name": "order_metainfo_id", "type": "string" },
                { "name": "order_metainfo_id_copy", "type": "string" },
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
    }]]),
    service_fields = {
        user = {
            {name = 'expires_on', type = 'long', default = 0},
        },
        order = {},
        order_metainfo = {},
    },
    indexes = {
        user_collection = {
            user_id_index = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            bucket_id = {
                service_fields = {},
                fields = {'bucket_id'},
                index_type = 'tree',
                unique = false,
                primary = false,
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
            bucket_id = {
                service_fields = {},
                fields = {'bucket_id'},
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
            order_metainfo_id_copy_index = {
                service_fields = {},
                fields = {'order_metainfo_id_copy'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
            order_id_index = {
                service_fields = {},
                fields = {'order_id'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
            bucket_id = {
                service_fields = {},
                fields = {'bucket_id'},
                index_type = 'tree',
                unique = false,
                primary = false,
            },
        }
    },
    vshard = {
        user_collection = {
            key_fields = {'user_id'},
            get_bucket_id = get_bucket_id,
            bucket_id_field = 'bucket_id',
            bucket_local_connections = {
                'order_connection',
            }
        },
        order_collection = {
            key_fields = {'user_id'},
            get_bucket_id = get_bucket_id,
            bucket_id_field = 'bucket_id',
            bucket_local_connections = {
                'user_connection',
                'order_metainfo_connection',
            }
        },
        order_metainfo_collection = {
            bucket_id_field = 'bucket_id',
        },
    }
}

local _, user_schema_handler =
    avro_schema.create(test_metadata.schemas.user)
local ok, user_schema = avro_schema.compile({
    user_schema_handler,
    service_fields = {'long'}
})
assert(ok, user_schema)

local _, order_schema_handler =
    avro_schema.create(test_metadata.schemas.order)
local ok, order_schema = avro_schema.compile(order_schema_handler)
assert(ok, order_schema)

local function fill_data(virtbox)
    local users = {
        {
            user_id = 'user_id_1',
            first_name = 'Ivan',
            middle_name = 'Ivanovich',
            last_name = 'Ivanov',
        },
        {
            user_id = 'user_id_2',
            first_name = 'Vasiliy',
            middle_name = 'Pupp',
            last_name = 'Pupkin',
        },
        {
            user_id = 'user_id_101',
            first_name = 'Иван',
            middle_name = 'Иванович',
            last_name = 'Иванов',
        },
    }

    local orders = {
        {
            order_id = 'order_id_1',
            user_id = 'user_id_1',
            description = 'first order of Ivan',
            price = 0,
            discount = 0,
            in_stock = true,
        },
        {
            order_id = 'order_id_2',
            user_id = 'user_id_1',
            description = 'second order of Ivan',
            price = 0,
            discount = 0,
            in_stock = false,
        },
        {
            order_id = 'order_id_3',
            user_id = 'user_id_2',
            description = 'first order of Vasiliy',
            price = 0,
            discount = 0,
            in_stock = true,
        },
        {
            order_id = 'order_id_3924',
            user_id = 'user_id_101',
            description = 'Покупка 3924',
            price = 0,
            discount = 0,
            in_stock = true,
        }
    }
    for _, user in ipairs(users) do
        virtbox.user_collection:replace_object(user, {1827767717})
    end
    for _, order in ipairs(orders) do
        virtbox.order_collection:replace_object(order)
    end

    for i = 3, 100 do
        virtbox.user_collection:replace_object({
            user_id = 'user_id_' .. i,
            first_name = 'first name ' .. i,
            middle_name = 'middle name ' .. i,
            last_name = 'last name ' .. i,
        }, {1827767717})
        for j = (4 + (i - 3) * 40), (4 + (i - 2) * 40) - 1 do
            virtbox.order_collection:replace_object({
                order_id = 'order_id_' .. j,
                user_id = 'user_id_' .. i,
                description = 'order of user ' .. i,
                price = i + j / 3,
                discount = i + j / 3,
                in_stock = j % 2 == 1,
            })
        end
    end
end

local function init_spaces()
    local format = {
        {'expires_on', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'user_id', 'string'},
        {'first_name', 'string'},
        {name='middle_name', type='string'},
        {'last_name', 'string'},
    }
    local s = box.schema.create_space('user_collection', {format = format})
    s:create_index('user_id_index', {parts = {{'user_id'}}})
    s:create_index('bucket_id', {parts = {{'bucket_id'}}, unique = false})

    local format = {
        {'bucket_id', 'unsigned'},
        {'order_id', 'string'},
        {'user_id', 'string'},
        {'description', 'string'},
        {'price', 'number'},
        {'discount', 'number'},
        {'in_stock', 'boolean'},
    }
    local s = box.schema.create_space('order_collection', {format = format})
    s:create_index('order_id_index', {parts = {{'order_id'}}})
    s:create_index('user_id_index', {parts = {{'user_id'}}, unique = false})
    s:create_index('bucket_id', {parts = {{'bucket_id'}}, unique = false})

    -- order_metainfo_collection fields
    local M_ORDER_BUCKET_ID_FN = 1
    local M_ORDER_METAINFO_ID_FN = 3
    local M_ORDER_METAINFO_ID_COPY_FN = 4
    local M_ORDER_ID_FN = 5
    local s = box.schema.create_space('order_metainfo_collection')
    s:create_index(
        'order_metainfo_id_index',
        {type = 'tree', parts = {
            M_ORDER_METAINFO_ID_FN, 'string'
        }}
    )
    s:create_index(
        'order_metainfo_id_copy_index',
        {type = 'tree', parts = {
            M_ORDER_METAINFO_ID_COPY_FN, 'string'
        }}
    )
    s:create_index('order_id_index',
        {type = 'tree', parts = {
            M_ORDER_ID_FN, 'string'
        }}
    )
    s:create_index('bucket_id',
        {type = 'tree', parts = {
            M_ORDER_BUCKET_ID_FN, 'unsigned'
        }, unique = false}
    )
end

return {
    meta = test_metadata,
    fill_data = fill_data,
    init_spaces = init_spaces,
}

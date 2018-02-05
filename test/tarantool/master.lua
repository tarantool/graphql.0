#!/usr/bin/env tarantool

box.cfg({
    listen = os.getenv('LISTEN'),
})

function instance_uri(instance_id)
    local SOCKET_DIR = require('fio').cwd()
    return ('%s/shard%s.sock'):format(SOCKET_DIR, instance_id)
end

function init_shard(servers, config)
    local shard = require('shard')
    local env = require('test_run')
    local test_run = env.new()

    test_run:create_cluster(SERVERS, 'tarantool')
    shard.init(config)
    shard.wait_connection()
end

function fill_test_data()
    local shard = require('shard')

    shard.user_collection:replace(
        {'user_id_1', 'Ivan', 'Ivanov'})
    shard.user_collection:replace(
        {'user_id_2', 'Vasiliy', 'Pupkin'})
    shard.order_collection:replace(
        {'order_id_1', 'user_id_1', 'first order of Ivan'})
    shard.order_collection:replace(
        {'order_id_2', 'user_id_1', 'second order of Ivan'})
    shard.order_collection:replace(
        {'order_id_3', 'user_id_2', 'first order of Vasiliy'})

    for i = 3, 100 do
        local s = tostring(i)
        shard.user_collection:replace(
            {'user_id_' .. s, 'first name ' .. s, 'last name ' .. s})
        for j = (4 + (i - 3) * 40), (4 + (i - 2) * 40) - 1 do
            local t = tostring(j)
            shard.order_collection:replace(
                {'order_id_' .. t, 'user_id_' .. s, 'order of user ' .. s})
        end
    end
end

function get_test_metadata()
    local schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                { "name": "user_id", "type": "string" },
                { "name": "first_name", "type": "string" },
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
        user = {},
        order = {},
    }

    local indexes = {
        user_collection = {
            user_id_index = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = true,
            },
        },
        order_collection = {
            order_id_index = {
                service_fields = {},
                fields = {'order_id'},
                index_type = 'tree',
                unique = true,
            },
            user_id_index = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = false,
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

require('console').listen(os.getenv('ADMIN'))

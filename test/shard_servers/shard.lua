#!/usr/bin/env tarantool

-- get instance name from filename (shard1.lua => 1)
local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()

local function instance_uri(instance_id)
    return ('%s/shard%s.sock'):format(SOCKET_DIR, instance_id)
end

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = instance_uri(INSTANCE_ID),
})

box.once('shard_init', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

-- user_collection fields
local U_USER_ID_FN = 1

-- order_collection fields
local O_ORDER_ID_FN = 1
local O_USER_ID_FN = 2

box.once('shard_init_spaces', function()
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

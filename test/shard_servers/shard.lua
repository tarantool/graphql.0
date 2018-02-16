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

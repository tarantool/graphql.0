#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local graphql_storage = require('graphql.storage')

local SOCKET_DIR = fio.cwd()
local function instance_uri(instance_id)
    return ('%s/shard%d.sock'):format(SOCKET_DIR, instance_id)
end

-- get instance name from filename (shard1.lua => 1)
local INSTANCE_ID = tonumber(string.match(arg[0], "%d"))
local INSTANCE_URI = instance_uri(INSTANCE_ID)

-- start console first
require('console').listen(os.getenv('ADMIN'))

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path
local vutils = require('test.vshard.vshard_utils')

local INSTANCE_UUID = vutils.id_to_uuid[INSTANCE_ID]
local REPLICASET_UUID = vutils.id_to_replicaset_uuid[INSTANCE_ID]
box.cfg({
    wal_mode = 'none',
    instance_uuid = INSTANCE_UUID,
    replicaset_uuid = REPLICASET_UUID,
    listen = INSTANCE_URI,
})
graphql_storage.init()

box.once('storage_init', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
    -- Crutch which fixes tarantool:#3492.
    box.schema.user.grant('guest', 'replication', nil, nil,
                          {if_not_exists = true})
end)

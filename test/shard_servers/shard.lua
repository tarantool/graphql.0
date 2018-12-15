#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local graphql_storage = require('graphql.storage')

-- get instance name from filename (shard1.lua => 1)
local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()

local function instance_uri(instance_id)
    return ('%s/shard%s.sock'):format(SOCKET_DIR, instance_id)
end

graphql_storage.init()

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({})

box.once('shard_init', function()
    box.schema.user.grant('guest', 'read,write,execute,create,alter,drop',
        'universe')
end)

box.cfg({
    listen = instance_uri(INSTANCE_ID),
})

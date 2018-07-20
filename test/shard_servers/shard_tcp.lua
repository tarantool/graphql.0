#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local graphql_storage = require('graphql.storage')

graphql_storage.init()

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = os.getenv('LISTEN'),
})

box.once('shard_init', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

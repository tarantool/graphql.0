#!/usr/bin/env tarantool

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = os.getenv('LISTEN'),
})

box.once('shard_init', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

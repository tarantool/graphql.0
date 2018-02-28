#!/usr/bin/env tarantool

box.cfg({
    listen = os.getenv('LISTEN'),
})

function instance_uri(instance_id)
    local SOCKET_DIR = require('fio').cwd()
    return ('%s/shard%s.sock'):format(SOCKET_DIR, instance_id)
end

function init_shard(servers, config, suite)
    local shard = require('shard')
    local env = require('test_run')
    local test_run = env.new()

    test_run:create_cluster(servers, suite)
    box.once('init_shard_module', function()
        shard.init(config)
    end)
    shard.wait_connection()
end

require('console').listen(os.getenv('ADMIN'))

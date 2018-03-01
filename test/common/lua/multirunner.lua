#!/usr/bin/env tarantool

local net_box = require('net.box')

local initialized = false

local function instance_uri(instance_id)
    local socket_dir = require('fio').cwd()
    local uri = ('%s/shard%s.sock'):format(socket_dir, instance_id)
    return uri
end

local function init_shard(test_run, servers, config)
    local suite = 'common'
    test_run:create_cluster(servers, suite)

    -- XXX: for now we always use one shard configuration (a first one),
    -- because it is unclear how to reload shard module with an another
    -- configuration; the another way is use several test configurations, but
    -- it seems to be non-working in 'core = app' tests with current test-run.
    -- Ways to better handle this is subject to future digging.
    local shard = require('shard')
    if not initialized then
        shard.init(config)
        initialized = true
    end
    shard.wait_connection()
    return shard
end

local function shard_cleanup(test_run, servers)
    test_run:drop_cluster(servers)
end

local function for_each_server(shard, func)
    for _, zone in ipairs(shard.shards) do
        for _, node in ipairs(zone) do
            func(node.uri)
        end
    end
end

-- Run tests on multiple accessors and configurations.
-- Feel free to add more configurations.
local function run(test_run, init_function, cleanup_function, callback)
    -- convert functions to string, so, that it can be executed on shards
    local init_script = string.dump(init_function)
    local cleanup_script = string.dump(cleanup_function)

    local servers = {'shard1', 'shard2', 'shard3', 'shard4'};

    -- Test sharding with redundancy = 2.
    local shard = init_shard(test_run, servers, {
        servers = {
            { uri = instance_uri('1'), zone = '0' },
            { uri = instance_uri('2'), zone = '1' },
            { uri = instance_uri('3'), zone = '2' },
            { uri = instance_uri('4'), zone = '3' },
        },
        login = 'guest',
        password = '',
        redundancy = 2,
        monitor = false
    })

    for_each_server(shard, function(uri)
        local c = net_box.connect(uri)
        c:eval(init_script)
    end)

    callback("Shard 2x2", shard)

    for_each_server(shard, function(uri)
        local c = net_box.connect(uri)
        c:eval(cleanup_script)
    end)
    shard_cleanup(test_run, servers)

    -- Test sharding without redundancy.
    local shard = init_shard(test_run, servers, {
        servers = {
            { uri = instance_uri('1'), zone = '0' },
            { uri = instance_uri('2'), zone = '1' },
            { uri = instance_uri('3'), zone = '2' },
            { uri = instance_uri('4'), zone = '3' },
        },
        login = 'guest',
        password = '',
        redundancy = 1,
        monitor = false
    })

    for_each_server(shard, function(uri)
        local c = net_box.connect(uri)
        c:eval(init_script)
    end)

    callback("Shard 4x1", shard)

    for_each_server(shard, function(uri)
        local c = net_box.connect(uri)
        c:eval(cleanup_script)
    end)
    shard_cleanup(test_run, servers)

    -- Test local setup (box).
    loadstring(init_script)()
    callback("Local (box)", nil)
    loadstring(cleanup_script)()
end

return {
    run = run
}

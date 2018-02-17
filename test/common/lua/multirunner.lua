#!/usr/bin/env tarantool
local net_box = require('net.box')
local shard = require('shard')

local function instance_uri(instance_id)
    local socket_dir = require('fio').cwd()
    local uri = ('%s/shard%s.sock'):format(socket_dir, instance_id)
    return uri
end

local function init_shard(test_run, servers, config)
    local shard = require('shard')
    local suite = "common"
    test_run:create_cluster(servers, suite)
    box.once('init_shard_module', function()
        shard.init(config)
    end)
    shard.wait_connection()
end

local function shard_cleanup(test_run, servers)
    test_run:drop_cluster(servers)
    -- crutch!!
    -- test_run.lua do not delete servers
    -- todo: should be fixed after in tesr-run
    local drop_cluster_cmd3 = 'delete server %s'
    for _, name in ipairs(servers) do
        test_run:cmd(drop_cluster_cmd3:format(name))
    end
end

-- Run tests on multiple accessors and configurations.
-- Feel free to add more configurations.
local function run(test_run, init_function, cleanup_function, callback)
    -- convert functions to string, so, that it can be executed on shards
    local init_script = string.dump(init_function)
    local cleanup_script = string.dump(cleanup_function)

    local servers = {'shard1', 'shard2', 'shard3', 'shard4'};

    -- Test sharding without redundancy = 2.
    init_shard(test_run, servers, {
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
    });

    for _, shard_server in ipairs(shard_status().online) do
        local c = net_box.connect(shard_server.uri)
        c:eval(init_script)
    end

    callback("Shard 2x2", shard)

    for _, shard_server in ipairs(shard_status().online) do
        local c = net_box.connect(shard_server.uri)
        c:eval(cleanup_script)
    end
    shard_cleanup(test_run, servers)

    -- Test sharding without redundancy.
    init_shard(test_run, servers, {
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
    });

    for _, shard_server in ipairs(shard_status().online) do
        local c = net_box.connect(shard_server.uri)
        c:eval(init_script)
    end

    callback("Shard 4x1", shard)

    for _, shard_server in ipairs(shard_status().online) do
        local c = net_box.connect(shard_server.uri)
        c:eval(cleanup_script)
    end
    shard_cleanup(test_run, servers)

    -- Test local setup (box).
    loadstring(init_script)()
    callback("Local (box)", nil)
    loadstring(cleanup_script)()
end

return {
    run = run
}

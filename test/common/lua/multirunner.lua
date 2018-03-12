#!/usr/bin/env tarantool

local net_box = require('net.box')

-- require in-repo version of graphql/ sources despite current working directory
local fio = require('fio')
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

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
    shard.init(config)
    shard.wait_connection()
    return shard
end

local function unload_shard()
    package.loaded["shard"] = nil
    _G.append_shard     = nil
    _G.join_shard        = nil
    _G.unjoin_shard      = nil
    _G.resharding_status = nil
    _G.remote_append     = nil
    _G.remote_join       = nil
    _G.remote_unjoin     = nil

    _G.find_operation    = nil
    _G.transfer_wait     = nil

    _G.cluster_operation = nil
    _G.execute_operation = nil
    _G.force_transfer    = nil
    _G.get_zones         = nil
    _G.merge_sort        = nil
    _G.shard_status      = nil
    _G.remote_resharding_state = nil
    for _, space in ipairs({
            '_shard_operations',
            '_shard_worker',
            '_shard',
            '_shard_worker_vinyl'
        }) do
        box.space[space]:drop()
    end
    for _, once in ipairs({
            "onceshard_init_v01",
            "onceshard_init_v02",
            "onceshard_init_v03",
        }) do
        box.space._schema:delete{once}
    end

end

local function shard_cleanup(test_run, servers)
    test_run:drop_cluster(servers)
    unload_shard()
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
    -- This crutch is necessary because we need to reloas shard module. However
    -- graphql do `require('shard')` for its own and we have to reload it too.
    local graphql = require('graphql')
    callback("Shard 2x2", shard, graphql)

    for_each_server(shard, function(uri)
        local c = net_box.connect(uri)
        c:eval(cleanup_script)
    end)
    package.loaded['graphql'] = nil
    shard_cleanup(test_run, servers)
    graphql = require('graphql')
    require('fiber').sleep(0.5)

    -- Test sharding without redundancy.
    shard = init_shard(test_run, servers, {
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

    callback("Shard 4x1", shard, graphql)

    for_each_server(shard, function(uri)
        local c = net_box.connect(uri)
        c:eval(cleanup_script)
    end)
    package.loaded['graphql'] = nil
    shard_cleanup(test_run, servers)
    graphql = require('graphql')

    -- Test local setup (box).
    loadstring(init_script)()
    callback("Local (box)", nil, graphql)
    loadstring(cleanup_script)()
end

return {
    run = run
}

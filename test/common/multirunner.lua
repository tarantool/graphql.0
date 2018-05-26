#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../../?.lua' .. ';' ..
    package.path

local net_box = require('net.box')
local utils = require('graphql.utils')

local function instance_uri(instance_id)
    local socket_dir = require('fio').cwd()
    local uri = ('%s/shard%s.sock'):format(socket_dir, instance_id)
    return uri
end

-- list on server names (server name + '.lua' is a server script) to pass to
-- test_run:create_cluster()
local SERVERS_DEFAULT = {'shard1', 'shard2', 'shard3', 'shard4'}

-- configuration to pass to shard.init(); `servers` variable contains a listen
-- URI of each storage and zone names
local SHARD_CONF_DEFAULT = {
    servers = {
        { uri = instance_uri('1'), zone = '0' },
        { uri = instance_uri('2'), zone = '1' },
        { uri = instance_uri('3'), zone = '2' },
        { uri = instance_uri('4'), zone = '3' },
    },
    login = 'guest',
    password = '',
    redundancy = 1,
    monitor = false,
}

local CONFS = {
    shard_2x2 = {
        type = 'shard',
        servers = SERVERS_DEFAULT,
        shard_conf = utils.merge_tables(SHARD_CONF_DEFAULT, {
            redundancy = 2,
        }),
    },
    shard_4x1 = {
        type = 'shard',
        servers = SERVERS_DEFAULT,
        shard_conf = utils.merge_tables(SHARD_CONF_DEFAULT, {
            redundancy = 1,
        }),
    },
    space = {
        type = 'space',
    },
}

local initialized = false

local function init_shard(test_run, servers, config, use_tcp)
    assert(initialized == false)

    local suite = 'common'
    local uris = test_run:create_cluster(servers, suite, {
        return_listen_uri = use_tcp,
    })

    -- we don't know ports before create cluster
    if use_tcp then
        config = table.copy(config)
        config.servers = {}
        for i = 1, 4 do
            config.servers[i] = {uri = uris[i], zone = tostring(i)}
        end
    end

    local shard = require('shard')
    shard.init(config)
    shard.wait_connection()
    initialized = true
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

local function run_conf(conf_name, opts)
    local conf = CONFS[conf_name]

    assert(conf ~= nil)

    local conf_type = conf.type
    local servers = conf.servers
    local shard_conf = conf.shard_conf

    assert(conf_type ~= nil)
    if conf_type == 'shard' then
        assert(servers ~= nil)
        assert(shard_conf ~= nil)
    end

    local test_run = opts.test_run
    local init_function = opts.init_function
    local init_function_params = opts.init_function_params or {}
    local cleanup_function = opts.cleanup_function
    local cleanup_function_params = opts.cleanup_function_params or {}
    local workload = opts.workload
    local servers = opts.servers or servers -- prefer opts.servers
    local use_tcp = opts.use_tcp or false

    assert(init_function ~= nil)
    assert(type(init_function_params) == 'table')
    assert(cleanup_function ~= nil)
    assert(type(cleanup_function_params) == 'table')
    assert(workload ~= nil)
    assert(use_tcp ~= nil)
    if conf_type == 'shard' then
        assert(test_run ~= nil)
        assert(servers ~= nil)
    end

    local result

    if conf_type == 'space' then
        init_function(unpack(init_function_params))
        result = workload(conf_name, nil)
        cleanup_function(unpack(cleanup_function_params))
    elseif conf_type == 'shard' then
        -- convert functions to string, so, that it can be executed on shards
        local init_script = string.dump(init_function)
        local cleanup_script = string.dump(cleanup_function)

        local shard = init_shard(test_run, servers, shard_conf, use_tcp)

        for_each_server(shard, function(uri)
            local c = net_box.connect(uri)
            c:eval(init_script, init_function_params)
        end)

        result = workload(conf_name, shard)

        for_each_server(shard, function(uri)
            local c = net_box.connect(uri)
            c:eval(cleanup_script, cleanup_function_params)
        end)

        shard_cleanup(test_run, servers)
    else
        assert(false, 'unknown conf_type: ' .. tostring(conf_type))
    end

    return result
end

local function get_conf(conf_name)
    local conf = CONFS[conf_name]
    assert(conf ~= nil)
    return conf
end

return {
    run_conf = run_conf,
    get_conf = get_conf,
}

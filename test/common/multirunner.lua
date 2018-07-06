#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../../?.lua' .. ';' ..
    package.path

local fiber = require('fiber')
local net_box = require('net.box')
local log = require('log')
local shard = require('shard')

local function instance_uri(instance_type, instance_id)
    local socket_dir = require('fio').cwd()
    local uri = ('%s/%s_%s.sock'):format(socket_dir, instance_type, instance_id)
    return uri
end

-- list on server names (server name + '.lua' is a server script) to pass to
-- test_run:create_cluster()
local SERVERS_DEFAULT = {
    shard_2x2 = {'shard_2x2_1', 'shard_2x2_2', 'shard_2x2_3', 'shard_2x2_4'},
    shard_4x1 = {'shard_4x1_1', 'shard_4x1_2', 'shard_4x1_3', 'shard_4x1_4'},
}

local CLUSTER_SERVERS_DEFAULT = {
    shard_2x2 = {
        { uri = instance_uri('shard_2x2', '1'), zone = '0' },
        { uri = instance_uri('shard_2x2', '2'), zone = '1' },
        { uri = instance_uri('shard_2x2', '3'), zone = '2' },
        { uri = instance_uri('shard_2x2', '4'), zone = '3' },
    },
    shard_4x1 = {
        { uri = instance_uri('shard_4x1', '1'), zone = '0' },
        { uri = instance_uri('shard_4x1', '2'), zone = '1' },
        { uri = instance_uri('shard_4x1', '3'), zone = '2' },
        { uri = instance_uri('shard_4x1', '4'), zone = '3' },
    },
}

-- configuration to pass to shard.init(); `servers` variable contains a listen
-- URI of each storage and zone names
local SHARD_CONF_DEFAULT = {
    shard_2x2 = {
        servers = CLUSTER_SERVERS_DEFAULT['shard_2x2'],
        login = 'guest',
        password = '',
        redundancy = 2,
        monitor = false,
    },
    shard_4x1 = {
        servers = CLUSTER_SERVERS_DEFAULT['shard_4x1'],
        login = 'guest',
        password = '',
        redundancy = 1,
        monitor = false,
    },
}

local CONFS = {
    shard_2x2 = {
        type = 'shard',
        servers = SERVERS_DEFAULT['shard_2x2'],
        shard_conf = SHARD_CONF_DEFAULT['shard_2x2'],
    },
    shard_4x1 = {
        type = 'shard',
        servers = SERVERS_DEFAULT['shard_4x1'],
        shard_conf = SHARD_CONF_DEFAULT['shard_4x1'],
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

    -- wait for execute grant
    for _, node in ipairs(config.servers) do
        local iter = 0
        while true do
            local c = net_box.connect(node.uri)
            local ok, res = pcall(c.eval, c, 'return 1 + 1')
            if ok then
                assert(res == 2)
                break
            end
            fiber.sleep(0.001)
            iter = iter + 1
            if iter % 1000 == 0 then
                log.info(('waiting for execute grant for %s; iter: %d'):format(
                    node.uri, iter))
            end
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

local function major_shard_version()
    return type(shard.wait_for_shards_to_go_online) == 'function' and 2 or 1
end

local function for_each_replicaset(shard, func)
    local only_master = major_shard_version() == 2 or
        shard.pool.configuration.replication

    for _, zone in ipairs(shard.shards) do
        for i = #zone, 1, -1 do
            local node = zone[i]
            func(node.uri)
            if only_master then
                break
            end
        end
    end
end

local function log_file(str)
    local ffi = require('ffi')
    ffi.cdef([[
        int getpid(void);
    ]])
    local pid = ffi.C.getpid()
    local SCRIPT_DIR = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
        :gsub('/./', '/'):gsub('/+$', ''))
    local file_name = 'log.log'
    local file_path = fio.abspath(fio.pathjoin(SCRIPT_DIR, '../..', file_name))
    local open_flags = {'O_WRONLY', 'O_CREAT', 'O_APPEND'}
    local fh, err = fio.open(file_path, open_flags, tonumber('644', 8))
    assert(fh ~= nil, ('open("%s", ...) error: %s'):format(file_path,
        tostring(err)))
    fh:write(('[%d] %s\n'):format(pid, str))
    fh:close()
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

        for_each_replicaset(shard, function(uri)
            local c = net_box.connect(uri)
            c:eval(init_script, init_function_params)
            -- XXX: make sync on the node where changes are made when we'll
            --      use stored procedures instead of box.space bindings
            if conf_name == 'shard_2x2' then
                log_file('init_script: call wait_for_replicas')
                c:call('wait_for_replicas')
                log_file('init_script: done wait_for_replicas')
            end
        end)

        result = workload(conf_name, shard)

        for_each_replicaset(shard, function(uri)
            local c = net_box.connect(uri)
            c:eval(cleanup_script, cleanup_function_params)
            -- XXX: make sync on the node where changes are made when we'll
            --      use stored procedures instead of box.space bindings
            if conf_name == 'shard_2x2' then
                log_file('cleanup_script: call wait_for_replicas')
                c:call('wait_for_replicas')
                log_file('cleanup_script: done wait_for_replicas')
            end
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

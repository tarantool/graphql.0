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
    vshard = {
        type = 'vshard',
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

-- @param meta Table with schemas, indexes, connections...
local function run_conf(conf_name, opts)
    local conf = CONFS[conf_name]

    assert(conf ~= nil)
    local ctx = {
        conf_name = conf_name,
        conf_type = conf.type,
        meta = opts.meta,
    }

    local servers = conf.servers
    local shard_conf = conf.shard_conf

    assert(ctx.conf_type ~= nil)
    if ctx.conf_type == 'shard' then
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
    if ctx.conf_type == 'shard' then
        assert(test_run ~= nil)
        assert(servers ~= nil)
    end

    local result

    if ctx.conf_type == 'space' then
        init_function(unpack(init_function_params))
        result = workload(ctx)
        cleanup_function(unpack(cleanup_function_params))
    elseif ctx.conf_type == 'shard' then
        -- convert functions to string, so, that it can be executed on shards
        local init_script = string.dump(init_function)
        local cleanup_script = string.dump(cleanup_function)

        local shard = init_shard(test_run, servers, shard_conf, use_tcp)

        for_each_server(shard, function(uri)
            local c = net_box.connect(uri)
            c:eval(init_script, init_function_params)
        end)

        ctx.shard = shard
        result = workload(ctx)

        for_each_server(shard, function(uri)
            local c = net_box.connect(uri)
            c:eval(cleanup_script, cleanup_function_params)
        end)

        shard_cleanup(test_run, servers)
    elseif ctx.conf_type == 'vshard' then
        local vutils = require('test.vshard.vshard_utils')
        assert(use_tcp == false)
        local SERVERS = {'shard1', 'shard2', 'shard3', 'shard4'}
        test_run:create_cluster(SERVERS, 'common')
        -- TODO: make params a dict.
        -- Offset for vshard specific fields.
        init_function_params[2] = 1
        require('fiber').sleep(1)
        vutils.cluster_setup()
        vutils.cluster_eval(init_function, init_function_params)
        vutils.cluster_eval(vutils.create_bucket_id_indexes,
            {vutils.get_bucket_id_positions(ctx.meta)})

        local vshard = require('vshard')
        local ok
        ok, ctx.router = pcall(vshard.router.new, 'my_router', vutils.cfg)
        ctx.router:bootstrap()
        assert(ok, ctx.router)
        assert(ctx.router)
        ctx.meta = table.deepcopy(ctx.meta)
        vutils.patch_non_vshard_meta(ctx.meta)
        result = workload(ctx)

        test_run:drop_cluster(SERVERS)
    else
        assert(false, 'unknown conf_type: ' .. tostring(ctx.conf_type))
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

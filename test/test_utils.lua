--- Various utility function used across the graphql module tests.

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../?.lua' .. ';' .. package.path

local log = require('log')
local yaml = require('yaml')
local avro_schema = require('avro_schema')
local digest = require('digest')
local utils = require('graphql.utils')
local shard = utils.optional_require('shard')
local graphql = require('graphql')
local multirunner = require('test.common.multirunner')
local test_run = utils.optional_require('test_run')
test_run = test_run and test_run.new()

local test_utils = {}

-- module-local variables
local models_cache

-- simplified version of the same named function from accessor_general.lua
function test_utils.compile_schemas(schemas, service_fields)
    local service_fields_types = {}
    for name, service_fields_list in pairs(service_fields) do
        local sf_types = {}
        local sf_defaults = {}
        for _, v in ipairs(service_fields_list) do
            sf_types[#sf_types + 1] = v.type
            sf_defaults[#sf_defaults + 1] = v.default
        end
        service_fields_types[name] = sf_types
    end

    local models = {}
    for name, schema in pairs(schemas) do
        local ok, handle = avro_schema.create(schema)
        assert(ok)
        local sf_types = service_fields_types[name]
        local ok, model = avro_schema.compile(
            {handle, service_fields = sf_types})
        assert(ok)
        models[name] = model
    end
    return models
end

local function get_model(meta, collection_name)
    local schema_name = meta.collections[collection_name].schema_name
    assert(schema_name ~= nil)
    if models_cache == nil then
        models_cache = test_utils.compile_schemas(meta.schemas,
            meta.service_fields)
    end
    local model = models_cache[schema_name]
    -- We don't handle case when there are two testdata modules with the
    -- same-named schemas within the one test; the result will be incorrect in
    -- the case.
    if model == nil then
        models_cache = test_utils.compile_schemas(meta.schemas,
            meta.service_fields)
        model = models_cache[schema_name]
    end
    assert(model ~= nil)
    return model
end

function test_utils.clear_models_cache()
    models_cache = nil
end

function test_utils.flatten_object(meta, collection_name, object,
        service_field_values)
    local model = get_model(meta, collection_name)
    local ok, tuple = model.flatten(object, unpack(service_field_values or {}))
    assert(ok, tostring(tuple))
    return tuple
end

function test_utils.unflatten_tuple(meta, collection_name, tuple)
    local model = get_model(meta, collection_name)
    local ok, object = model.unflatten(tuple)
    assert(ok, tostring(object))
    return object
end

function test_utils.replace_object(virtbox, meta, collection_name, object,
        service_field_values)
    local tuple = test_utils.flatten_object(meta, collection_name, object,
        service_field_values)
    virtbox[collection_name]:replace(tuple)
end

function test_utils.graphql_from_testdata(testdata, shard, graphql_opts)
    local graphql_opts = graphql_opts or {}
    local meta = testdata.meta or testdata.get_test_metadata()

    local default_graphql_opts = {
        schemas = meta.schemas,
        collections = meta.collections,
        service_fields = meta.service_fields,
        indexes = meta.indexes,
        accessor = shard and 'shard' or 'space',
    }

    -- allow to run under tarantool w/o additional opts w/o test-run
    local test_conf_graphql_opts = test_run and test_run:get_cfg('graphql_opts')
        or {}

    -- disable timeouts when we're run under luacov
    -- note: when a timeout is set and is tiny it is query_timeout.test.lua, so
    -- we should leave the timeout as is
    local luacov_graphql_opts = {}
    if rawget(_G, 'TEST_RUN_LUACOV') and (graphql_opts.timeout_ms == nil or
            graphql_opts.timeout_ms > 1) then
        luacov_graphql_opts = {timeout_ms = 100000}
    end

    local gql_wrapper = graphql.new(utils.merge_tables(
        default_graphql_opts, test_conf_graphql_opts, graphql_opts,
        luacov_graphql_opts))

    return gql_wrapper
end

function test_utils.get_conf_name()
    return test_run and test_run:get_cfg('conf') or 'space'
end

local replicasets_count = {
    space = 1,
    shard_2x2 = 2,
    shard_4x1 = 4,
}

function test_utils.get_replicasets_count()
    local conf_name = test_utils.get_conf_name()
    local res = replicasets_count[conf_name]
    assert(res)
    return res
end

function test_utils.get_executor_name()
    local conf_name = test_utils.get_conf_name()
    local on_shard = conf_name == 'shard_2x2' or conf_name == 'shard_4x1'
    local graphql_opts = test_run and test_run:get_cfg('graphql_opts') or {}
    local use_bfs_executor = graphql_opts.use_bfs_executor or 'shard'

    if use_bfs_executor == 'always' then
        return 'bfs'
    elseif use_bfs_executor == 'shard' then
        return on_shard and 'bfs' or 'dfs'
    end

    assert(use_bfs_executor == 'never')
    return 'dfs'
end

function test_utils.is_cache_supported()
    local conf_name = test_utils.get_conf_name()
    return conf_name == 'shard_2x2' or conf_name == 'shard_4x1'
end

function test_utils.run_testdata(testdata, opts)
    local opts = opts or {}
    local run_queries = opts.run_queries or testdata.run_queries
    -- custom workload for, say, test different options on several graphql
    -- instances
    local workload = opts.workload or nil

    -- allow to run under tarantool on 'space' configuration w/o test-run
    local conf_name = test_run and test_run:get_cfg('conf') or 'space'

    multirunner.run_conf(conf_name, {
        test_run = test_run,
        init_function = testdata.init_spaces,
        init_function_params = {},
        cleanup_function = testdata.drop_spaces,
        workload = function(conf_name, shard)
            if workload then
                workload(conf_name, shard)
            else
                local virtbox = shard or box.space
                local meta = testdata.meta or testdata.get_test_metadata()
                testdata.fill_test_data(virtbox, meta)
                local gql_wrapper = test_utils.graphql_from_testdata(testdata,
                    shard, opts.graphql_opts)
                run_queries(gql_wrapper, virtbox, meta)
            end
            test_utils.clear_models_cache()
        end,
        servers = {'shard1', 'shard2', 'shard3', 'shard4'},
        use_tcp = false,
    })
end

--- Log an error and the corresponding backtrace in case of the `func` function
--- call raises the error.
function test_utils.show_trace(func, ...)
    local args = {...}
    return select(2, xpcall(
        function() return func(unpack(args)) end,
        function(err)
            if type(err) == 'string' then
                log.info('ERROR: ' .. err)
            else
                log.info('ERROR:\n' .. yaml.encode(err))
            end
            log.info(debug.traceback())
        end
    ))
end

-- needed to compare a dump with floats/doubles, because, say,
-- `tonumber(tostring(1/3)) == 1/3` is `false`
function test_utils.deeply_number_tostring(t)
    if type(t) == 'table' then
        local res = {}
        for k, v in pairs(t) do
            res[k] = test_utils.deeply_number_tostring(v)
        end
        return res
    elseif type(t) == 'number' then
        return tostring(t)
    else
        return table.deepcopy(t)
    end
end

function test_utils.get_shard_key_hash(key)
    local shards_n = #shard.shards
    local num = type(key) == 'number' and key or digest.crc32(key)
    return 1 + digest.guava(num, shards_n)
end

function test_utils.test_conf_graphql_opts()
    -- allow to run under tarantool w/o additional opts w/o test-run
    return test_run and test_run:get_cfg('graphql_opts') or {}
end

return test_utils

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
local virtual_box = require('test.virtual_box')
local utils = require('graphql.utils')
local test_run = utils.optional_require('test_run')
test_run = test_run and test_run.new()

local test_utils = {}

function test_utils.major_avro_schema_version()
    local ok, handle = avro_schema.create('boolean')
    assert(ok)
    local ok, model = avro_schema.compile(handle)
    assert(ok)
    return model.get_types == nil and 2 or 3
end

function test_utils.graphql_from_testdata(testdata, graphql_opts, ctx)
    local graphql_opts = graphql_opts or {}
    local meta = ctx.meta

    local default_graphql_opts = {
        schemas = meta.schemas,
        collections = meta.collections,
        service_fields = meta.service_fields,
        indexes = meta.indexes,
        vshard = meta.vshard,
        accessor = ctx.conf_type,
        router = ctx.router,
    }

    -- allow to run under tarantool w/o additional opts w/o test-run
    local test_conf_graphql_opts = test_run and test_run:get_cfg('graphql_opts')
        or {}

    local gql_wrapper = graphql.new(utils.merge_tables(
        default_graphql_opts, test_conf_graphql_opts, graphql_opts))

    return gql_wrapper
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
        init_function_params = {test_utils.major_avro_schema_version()},
        cleanup_function = testdata.drop_spaces,
        meta = testdata.meta or testdata.get_test_metadata(),
        workload = function(ctx)
            local virtbox = virtual_box.get_virtbox_for_accessor(ctx.conf_type,
                ctx)
            -- ctx may contain:
            -- * router
            -- * meta
            -- * shard
            -- * conf_type
            if workload then
                workload(ctx, virtbox)
            else
                testdata.fill_test_data(virtbox)
                local gql_wrapper = test_utils.graphql_from_testdata(testdata,
                    opts.graphql_opts, ctx)
                run_queries(gql_wrapper, virtbox, ctx.meta)
            end
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

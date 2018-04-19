--- Various utility function used across the graphql module tests.

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../?.lua' .. ';' .. package.path

local yaml = require('yaml')
local graphql = require('graphql')
local multirunner = require('test.common.lua.multirunner')
local graphql_utils = require('graphql.utils')
local test_run = graphql_utils.optional_require('test_run')
test_run = test_run and test_run.new()

local utils = {}

function utils.format_result(name, query, variables, result)
    return ('RUN %s {{{\nQUERY\n%s\nVARIABLES\n%s\nRESULT\n%s\n}}}\n'):format(
    name, query:rstrip(), yaml.encode(variables), yaml.encode(result))
end

function utils.print_and_return(...)
    print(...)
    return table.concat({ ... }, ' ') .. '\n'
end

function utils.graphql_from_testdata(testdata, shard, graphql_opts)
    local graphql_opts = graphql_opts or {}
    local meta = testdata.meta or testdata.get_test_metadata()

    local default_graphql_opts = {
        schemas = meta.schemas,
        collections = meta.collections,
        service_fields = meta.service_fields,
        indexes = meta.indexes,
        accessor = shard and 'shard' or 'space',
    }

    local gql_wrapper = graphql.new(graphql_utils.merge_tables(
        default_graphql_opts, graphql_opts))

    return gql_wrapper
end

function utils.run_testdata(testdata, opts)
    local opts = opts or {}
    local run_queries = opts.run_queries or testdata.run_queries

    -- allow to run under tarantool on 'space' configuration w/o test-run
    local conf_name = test_run and test_run:get_cfg('conf') or 'space'

    multirunner.run_conf(conf_name, {
        test_run = test_run,
        init_function = testdata.init_spaces,
        cleanup_function = testdata.drop_spaces,
        workload = function(_, shard)
            local virtbox = shard or box.space
            testdata.fill_test_data(virtbox)
            local gql_wrapper = utils.graphql_from_testdata(testdata, shard,
                opts.graphql_opts)
            run_queries(gql_wrapper)
        end,
        servers = {'shard1', 'shard2', 'shard3', 'shard4'},
        use_tcp = false,
    })
end

return utils

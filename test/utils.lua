--- Various utility function used across the graphql module tests.

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../?.lua' .. ';' .. package.path

local log = require('log')
local avro_schema = require('avro_schema')
local graphql = require('graphql')
local multirunner = require('test.common.multirunner')
local graphql_utils = require('graphql.utils')
local test_run = graphql_utils.optional_require('test_run')
test_run = test_run and test_run.new()

local utils = {}

-- module-local variables
local models_cache

-- simplified version of the same named function from accessor_general.lua
function utils.compile_schemas(schemas, service_fields)
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
        models_cache = utils.compile_schemas(meta.schemas,
            meta.service_fields)
    end
    local model = models_cache[schema_name]
    -- We don't handle case when there are two testdata modules with the
    -- same-named schemas within the one test; the result will be incorrect in
    -- the case.
    if model == nil then
        models_cache = utils.compile_schemas(meta.schemas,
            meta.service_fields)
        model = models_cache[schema_name]
    end
    assert(model ~= nil)
    return model
end

function utils.clear_models_cache()
    models_cache = nil
end

function utils.flatten_object(meta, collection_name, object,
        service_field_values)
    local model = get_model(meta, collection_name)
    local ok, tuple = model.flatten(object, unpack(service_field_values or {}))
    assert(ok, tostring(tuple))
    return tuple
end

function utils.unflatten_tuple(meta, collection_name, tuple)
    local model = get_model(meta, collection_name)
    local ok, object = model.unflatten(tuple)
    assert(ok, tostring(object))
    return object
end

function utils.replace_object(virtbox, meta, collection_name, object,
        service_field_values)
    local tuple = utils.flatten_object(meta, collection_name, object,
        service_field_values)
    virtbox[collection_name]:replace(tuple)
end

function utils.major_avro_schema_version()
    local ok, handle = avro_schema.create('boolean')
    assert(ok)
    local ok, model = avro_schema.compile(handle)
    assert(ok)
    return model.get_types == nil and 2 or 3
end

-- return an error w/o file name and line number
function utils.strip_error(err)
    local res = tostring(err):gsub('^.-:.-: (.*)$', '%1')
    return res
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
    -- custom workload for, say, test different options on several graphql
    -- instances
    local workload = opts.workload or nil

    -- allow to run under tarantool on 'space' configuration w/o test-run
    local conf_name = test_run and test_run:get_cfg('conf') or 'space'

    multirunner.run_conf(conf_name, {
        test_run = test_run,
        init_function = testdata.init_spaces,
        init_function_params = {utils.major_avro_schema_version()},
        cleanup_function = testdata.drop_spaces,
        workload = function(conf_name, shard)
            if workload then
                workload(conf_name, shard)
            else
                local virtbox = shard or box.space
                local meta = testdata.meta or testdata.get_test_metadata()
                testdata.fill_test_data(virtbox, meta)
                local gql_wrapper = utils.graphql_from_testdata(testdata, shard,
                    opts.graphql_opts)
                run_queries(gql_wrapper, virtbox, meta)
            end
            utils.clear_models_cache()
        end,
        servers = {'shard1', 'shard2', 'shard3', 'shard4'},
        use_tcp = false,
    })
end

--- Log an error and the corresponding backtrace in case of the `func` function
--- call raises the error.
function utils.show_trace(func, ...)
    local args = {...}
    return select(2, xpcall(
        function() return func(unpack(args)) end,
        function(err)
            log.info('ERROR: ' .. tostring(err))
            log.info(debug.traceback())
        end
    ))
end

return utils

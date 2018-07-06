--- Implementation of module-level functions and functions of instances of the
--- graphql library and a compiled query.

local accessor_space = require('graphql.accessor_space')
local accessor_shard = require('graphql.accessor_shard')
local accessor_general = require('graphql.accessor_general')
local parse = require('graphql.core.parse')
local validate = require('graphql.core.validate')
local execute = require('graphql.core.execute')
local query_to_avro = require('graphql.query_to_avro')
local simple_config = require('graphql.simple_config')
local config_complement = require('graphql.config_complement')
local server = require('graphql.server.server')
local convert_schema = require('graphql.convert_schema')

local utils = require('graphql.utils')
local check = utils.check

local impl = {}

-- Instance of the library to provide graphql:compile() and graphql:execute()
-- method (with creating zero configuration graphql instance under hood when
-- calling compile() for the first time).
local default_instance

--- Execute an operation from compiled query.
---
--- @tparam table qstate compiled query
---
--- @tparam table variables variables to pass to the query
---
--- @tparam[opt] string operation_name optional operation name
---
--- @treturn table result of the operation
local function gql_execute(qstate, variables, operation_name)
    assert(qstate.state)
    local state = qstate.state
    assert(state.schema)

    check(variables, 'variables', 'table')
    check(operation_name, 'operation_name', 'string', 'nil')

    assert(qstate.query_settings)
    local root_value = {}
    local qcontext = {
        query_settings = qstate.query_settings,
    }

    local traceback
    local ok, data = xpcall(function()
        return execute(state.schema, qstate.ast, root_value, variables,
            operation_name, {qcontext = qcontext})
    end, function(err)
        traceback = debug.traceback()
        return err
    end)
    if not ok then
        local err = utils.serialize_error(data, traceback)
        return {errors = {err}}
    end
    return {
        data = data,
        meta = {
            statistics = qcontext.statistics,
        }
    }
end

--- Compile a query and execute an operation.
---
--- See @{gql_compile} and @{gql_execute} for parameters description.
---
--- @treturn table result of the operation
local function compile_and_execute(state, query, variables, operation_name,
        opts)
    assert(type(state) == 'table', 'use :gql_execute(...) instead of ' ..
        '.execute(...)')
    assert(state.schema ~= nil, 'have not compiled schema')
    check(query, 'query', 'string')
    check(variables, 'variables', 'table', 'nil')
    check(operation_name, 'operation_name', 'string', 'nil')
    check(opts, 'opts', 'table', 'nil')

    local compiled_query = state:compile(query, opts)
    return compiled_query:execute(variables, operation_name)
end

--- Parse GraphQL query string, validate against the GraphQL schema and
--- provide an object with the function to execute an operation from the
--- request with specific variables values.
---
--- @tparam table state the library instance
---
--- @tparam string query text of a GraphQL query
---
--- @tparam[opt] table opts the following options (described in
--- @{accessor_general.new}):
---
--- * resulting_object_cnt_max
--- * fetched_object_cnt_max
--- * timeout_ms
---
--- @treturn table compiled query with `execute` and `avro_schema` functions
local function gql_compile(state, query, opts)
    assert(type(state) == 'table' and type(query) == 'string',
        'use :validate(...) instead of .validate(...)')
    assert(state.schema ~= nil, 'have not compiled schema')
    check(query, 'query', 'string')
    check(opts, 'opts', 'table', 'nil')

    local opts = opts or {}

    local ast = parse(query)
    validate(state.schema, ast)

    local qstate = {
        state = state,
        ast = ast,
        query_settings = {
            resulting_object_cnt_max = opts.resulting_object_cnt_max,
            fetched_object_cnt_max = opts.fetched_object_cnt_max,
            timeout_ms = opts.timeout_ms,
        }
    }

    accessor_general.validate_query_settings(qstate.query_settings,
        {allow_nil = true})

    local gql_query = setmetatable(qstate, {
        __index = {
            execute = gql_execute,
            avro_schema = query_to_avro.convert
        }
    })
    return gql_query
end

local function start_server(gql, host, port)
    assert(type(gql) == 'table',
        'use :start_server(...) instead of .start_server(...)')

    check(host, 'host', 'nil', 'string')
    check(port, 'port', 'nil', 'number')

    gql.server = server.init(gql, host, port)
    gql.server:start()

    return ('The GraphQL server started at http://%s:%s'):format(
        gql.server.host, gql.server.port
    )
end

local function stop_server(gql)
    assert(type(gql) == 'table',
        'use :stop_server(...) instead of .stop_server(...)')
    assert(gql.server, 'no running server to stop')

    gql.server:stop()

    return ('The GraphQL server stopped at http://%s:%s'):format(
        gql.server.host, gql.server.port)
end

--- The function creates an accessor of desired type with default configuration.
---
--- @tparam table cfg schemas, collections, service_fields, indexes and so on
---
--- @treturn table `accessor` created accessor instance
local function create_default_accessor(cfg)
    check(cfg.accessor, 'cfg.accessor', 'string')
    assert(cfg.accessor == 'space' or cfg.accessor == 'shard',
        'accessor_type must be shard or space, got ' .. cfg.accessor)
    check(cfg.service_fields, 'cfg.service_fields', 'table')
    check(cfg.indexes, 'cfg.indexes', 'table')
    check(cfg.collection_use_tomap, 'cfg.collection_use_tomap', 'table', 'nil')
    check(cfg.accessor_funcs, 'cfg.accessor_funcs', 'table', 'nil')

    local accessor_cfg = {
        schemas = cfg.schemas,
        collections = cfg.collections,
        service_fields = cfg.service_fields,
        indexes = cfg.indexes,
        collection_use_tomap = cfg.collection_use_tomap,
        resulting_object_cnt_max = cfg.resulting_object_cnt_max,
        fetched_object_cnt_max = cfg.fetched_object_cnt_max,
        timeout_ms = cfg.timeout_ms,
        enable_mutations = cfg.enable_mutations,
    }

    if cfg.accessor == 'space' then
        return accessor_space.new(accessor_cfg, cfg.accessor_funcs)
    end

    if cfg.accessor == 'shard' then
        return accessor_shard.new(accessor_cfg, cfg.accessor_funcs)
    end
end

function impl.compile(query)
    if default_instance == nil then
        default_instance = impl.new()
    end
    return default_instance:compile(query)
end

function impl.execute(query, variables, operation_name)
    if default_instance == nil then
        default_instance = impl.new()
    end
    return default_instance:execute(query, variables, operation_name)
end

function impl.start_server()
    if default_instance == nil then
        default_instance = impl.new()
    end

    return default_instance:start_server()
end

function impl.stop_server()
    if default_instance ~= nil and default_instance.server ~= nil then
        return default_instance:stop_server()
    end
    return 'there is no active server in default Tarantool graphql instance'
end

--- Create the library instance.
---
--- Usage:
---
--- ... = graphql.new({
---     schemas = {
---         schema_name_foo = { // the value is avro-schema (esp., a record)
---             name = 'schema_name_foo,
---             type = 'record',
---             fields = {
---                 ...
---             }
---         },
---         ...
---     },
---     collections = {
---         collections_name_foo = {
---             schema_name = 'schema_name_foo',
---             connections = { // the optional field
---                 {
---                     name = 'connection_name_bar',
---                     destination_collection = 'collection_baz',
---                     parts = {
---                         {
---                             source_field = 'field_name_source_1',
---                             destination_field = 'field_name_destination_1'
---                         },
---                         ...
---                     },
---                     index_name = 'index_name' -- is is for an accessor,
---                                               -- ignored in the graphql
---                                               -- part
---                 },
---                 ...
---             },
---         },
---         ...
---     },
---     indexes = <table>,
---     service_fields = <table>,
---     accessor = <table> or <string>,
---     accessor_funcs = <table>,
---     collection_use_tomap = <boolean>,
---     resulting_object_cnt_max = <number>,
---     fetched_object_cnt_max = <number>,
---     timeout_ms = <number>,
---     enable_mutations = <boolean>,
---     disable_dangling_check = <boolean>,
--- })
function impl.new(cfg)
    local cfg = cfg or {}
    cfg = table.deepcopy(cfg) -- prevent change of user's data

    -- auto config case
    if not next(cfg) or utils.has_only(cfg, 'connections') then
        local generated_cfg = simple_config.graphql_cfg_from_tarantool()
        generated_cfg.accessor = 'space'
        generated_cfg.connections = cfg.connections or {}
        cfg = generated_cfg
        cfg = config_complement.complement_cfg(cfg)
    end

    check(cfg.accessor, 'cfg.accessor', 'string', 'table')
    if type(cfg.accessor) == 'string' then
        cfg.accessor = create_default_accessor(cfg)
    end

    -- to use `cfg` as db_schema
    if cfg.service_fields == nil then
        cfg.service_fields = cfg.accessor.service_fields
    end

    -- to use `cfg` as db_schema
    if cfg.indexes == nil then
        cfg.indexes = cfg.accessor.indexes
    end

    local state = {
        disable_dangling_check = cfg.disable_dangling_check,
    }
    convert_schema.convert(state, cfg)
    return setmetatable(state, {
        __index = {
            compile = gql_compile,
            execute = compile_and_execute,
            start_server = start_server,
            stop_server = stop_server,
            internal = { -- for unit testing
                cfg = cfg,
            }
        }
    })
end

return impl

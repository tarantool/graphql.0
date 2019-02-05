--- Data accessor module that is base for `accessor_space` and
--- `accessor_shard` ones.
---
--- It provides basic logic for such space-like data storages and abstracted
--- away from details from where tuples are arrived into the application.

local ffi = require('ffi')
local json = require('json')
local avro_schema = require('avro_schema')
local utils = require('graphql.utils')
local clock = require('clock')
local rex = utils.optional_require_rex()
local db_schema_helpers = require('graphql.db_schema_helpers')
local error_codes = require('graphql.error_codes')
local statistics = require('graphql.statistics')
local expressions = require('graphql.expressions')
local constant_propagation = require('graphql.expressions.constant_propagation')
local find_index = require('graphql.find_index')

local check = utils.check
local e = error_codes

-- XXX: consider using [1] when it will be mature enough;
-- look into [2] for the status.
-- [1]: https://github.com/igormunkin/lua-re
-- [2]: https://github.com/tarantool/tarantool/issues/2764

local accessor_general = {}

local DEF_RESULTING_OBJECT_CNT_MAX = 10000
local DEF_FETCHED_OBJECT_CNT_MAX = 10000
local DEF_TIMEOUT_MS = 1000

-- We shold be able to multiply `timeout_ms` to 10^6 (convert to nanoseconds)
-- and add it to a `clock.monotonic64()` value; the result of that arithmetic
-- must not overflows. We can consider another approach to handle time limits:
-- save start time at start, calculate current time on each iteration and
-- substract the start time from it, compare with the timeout. With such
-- approch we don't add the timeout in nanoseconds to a start time and can
-- remove the divide by two below. The value is roughly equal to 292 years.
local TIMEOUT_INFINITY = 18446744073709551615ULL / (2 * 10^6) -- milliseconds

accessor_general.TIMEOUT_INFINITY = TIMEOUT_INFINITY

--- Validate and compile set of avro schemas (with respect to service fields).
---
--- @tparam table schemas map where keys are string names and values are
---               avro schemas; consider an example in @{impl.new}
---               function description.
--- @tparam table service_fields map where keys are string names of avro
---               schemas (from `schemas` argument) and values are service
---               fields descriptions; consider the example in the @{new}
---               function description.
---
--- @treturn table `models` list of compiled schemas
--- @treturn table `service_fields_defaults` list of default values of service
---                fields
local function compile_schemas(schemas, service_fields)
    local service_fields_types = {}
    local service_fields_defaults = {}
    for name, service_fields_list in pairs(service_fields) do
        assert(type(name) == 'string',
            'service_fields key must be a string, got ' .. type(name))
        assert(type(service_fields_list) == 'table',
            'service_fields_list must be a table, got ' ..
            type(service_fields_list))
        local sf_types = {}
        local sf_defaults = {}
        for _, v in ipairs(service_fields_list) do
            assert(type(v) == 'table',
                'service_fields_list item must be a table, got ' .. type(v))
            assert(type(v.name) == 'string',
                'service_field name must be a string, got ' .. type(v.name))
            assert(type(v.type) == 'string',
                'service_field type must be a string, got ' .. type(v.type))
            assert(v.default ~= nil, 'service_field default must not be a nil')
            sf_types[#sf_types + 1] = v.type
            sf_defaults[#sf_defaults + 1] = v.default
        end
        service_fields_types[name] = sf_types
        service_fields_defaults[name] = sf_defaults
    end

    local models = {}
    for name, schema in pairs(schemas) do
        assert(type(schema) == 'table',
            'avro_schema must be a table, got ' .. type(schema))
        assert(type(name) == 'string',
            'schema key must be a string, got ' .. type(name))
        assert(type(schema.name) == 'string',
            'schema name must be a string, got ' .. type(schema.name))
        assert(name == schema.name,
            ('schema key and name do not match: %s vs %s'):format(
            name, schema.name))
        assert(schema.type == 'record', 'schema.type must be a record')

        local ok, handle = avro_schema.create(schema)
        assert(ok, ('cannot create avro_schema for %s: %s'):format(
            name, tostring(handle)))
        local sf_types = service_fields_types[name]
        assert(sf_types ~= nil,
            ('cannot find service_fields for schema "%s"'):format(name))
        local ok, model = avro_schema.compile(
            {handle, service_fields = sf_types})
        assert(ok, ('cannot compile avro_schema for %s: %s'):format(
            name, tostring(model)))

        models[name] = model
    end
    return models, service_fields_defaults
end

--- Validate `from` parameter of accessor_instance:select().
---
--- @tparam table from see @{impl.new}
---
--- Raises an error when the validation fails.
---
--- @return nothing
local function validate_from_parameter(from)
    check(from, 'from', 'table')
    check(from.collection_name, 'from.collection_name', 'nil', 'string')
    check(from.connection_name, 'from.connection_name', 'string')
    check(from.destination_args_names, 'from.destination_args_names', 'table')
    check(from.destination_args_values, 'from.destination_args_values', 'table')
end

--- Set of asserts to check the `opts.collections` argument of the
--- @{accessor_general.new} function.
---
--- @tparam table collections map from collection names to collections as
--- defined in the @{accessor_general.new} function decription; this is subject
--- to validate
---
--- @tparam table schemas map from schema names to schemas as defined in the
--- @{impl.new} function; this is for validate collection against certain set
--- of schemas (no 'dangling' schema names in collections)
---
--- @return nil
local function validate_collections(collections, schemas)
    for collection_name, collection in pairs(collections) do
        assert(type(collection_name) == 'string',
            'collection_name must be a string, got ' ..
            type(collection_name))
        assert(type(collection) == 'table',
            'collection must be a table, got ' .. type(collection))
        local schema_name = collection.schema_name
        assert(type(schema_name) == 'string',
            'collection.schema_name must be a string, got ' ..
            type(schema_name))
        assert(schemas[schema_name] ~= nil,
            ('cannot find schema "%s" for collection "%s"'):format(
                schema_name, collection_name))
        local connections = collection.connections
        assert(connections == nil or type(connections) == 'table',
            'collection.connections must be nil or table, got ' ..
            type(connections))
        for _, connection in ipairs(connections) do
            assert(type(connection) == 'table',
                'connection must be a table, got ' .. type(connection))
            assert(type(connection.name) == 'string',
                'connection.name must be a string, got ' ..
                    type(connection.name))
            if connection.destination_collection then
                assert(type(connection.destination_collection) == 'string',
                    'connection.destination_collection must be a string, got ' ..
                    type(connection.destination_collection))
                assert(type(connection.parts) == 'table',
                    'connection.parts must be a string, got ' ..
                    type(connection.parts))
                assert(type(connection.index_name) == 'string',
                    'connection.index_name must be a string, got ' ..
                    type(connection.index_name))
            elseif connection.variants then
                for _, v in pairs(connection.variants) do
                    assert(type(v.determinant) == 'table', "variant's " ..
                        "determinant must be a table, got " ..
                        type(v.determinant))
                    assert(type(v.destination_collection) == 'string',
                        'variant.destination_collection must be a string, ' ..
                        'got ' .. type(v.destination_collection))
                    assert(type(v.parts) == 'table',
                        'variant.parts must be a table, got ' .. type(v.parts))
                    assert(type(v.index_name) == 'string',
                        'variant.index_name must be a string, got ' ..
                        type(v.index_name))
                end
            else
                assert(false, ('connection "%s" of collection "%s" does not ' ..
                    'have neither destination collection nor variants field'):
                    format(connection.name, collection_name))
            end
        end
    end
end

--- Whether an object match set of PCREs.
---
--- @tparam table pcre map with PCRE as values; names are correspond to field
--- names of the `obj` to match
---
--- @tparam table obj an object to check
---
--- @treturn boolean `res` whether the `obj` object match `pcre` set of
--- regexps.
local function match_using_re(pcre, obj)
    if pcre == nil then return true end

    assert(rex ~= nil, 'we should not pass over :compile() ' ..
        'with a query contains PCRE matching when there are '..
        'no lrexlib-pcre (rex_pcre) module present')

    for field_name, re in pairs(pcre) do
        -- skip an object with null in a string* field
        if obj[field_name] == nil then
            return false
        end
        if type(re) == 'table' then
            local match = match_using_re(re, obj[field_name])
            if not match then return false end
        elseif not utils.regexp(re, obj[field_name]) then
            return false
        end
    end

    return true
end

--- Whether an object match an expression.
---
--- @param expr (table or string) compiled or raw expression
---
--- @tparam table obj an object to check
---
--- @tparam[opt] table variables variables values from the request
---
--- @treturn boolean `res` whether the `obj` object match `expr` expression
local function match_using_expr(expr, obj, variables)
    if expr == nil then return true end

    check(expr, 'expression', 'table')
    local res = expr:execute(obj, variables)
    check(res, 'expression result', 'boolean')
    return res
end

--- Check whether we meet deadline time.
---
--- The functions raises an exception in the case.
---
--- @tparam table qcontext
---
--- @return nothing
local function check_deadline_clock(qcontext)
    if clock.monotonic64() > qcontext.deadline_clock then
        error(e.timeout_exceeded((
            'query execution timeout exceeded timeout_ms limit (%s ms)'):format(
            tostring(qcontext.query_settings.timeout_ms))))
    end
end

--- Perform unflatten, skipping, filtering, limiting of objects. This is the
--- core of the @{prepare_select_internal} function.
---
--- @tparam table self accessor_general instance
---
--- @tparam table state table variable where the function holds state accross
--- invokes; fields:
---
--- * `count` (number),
--- * `objs` (table, list of objects),
--- * `pivot_found` (boolean),
--- * `qcontext` (table, per-query local storage).
---
--- @tparam cdata tuple flatten representation of an object to process
---
--- @tparam table opts read only options:
---
--- * `model` (compiled avro schema),
--- * `limit` (number, max count of objects in result),
--- * `filter` (table, set of fields to match objects by equality),
--- * `do_filter` (boolean, whether we need to filter out non-matching
---   objects),
--- * `pivot_filter` (table, set of fields to match the objected pointed by
---   `offset` arqument of the GraphQL query),
--- * `resolveField` (function) for subrequests, see @{impl.new}.
--- * XXX: describe other fields.
---
--- @return nil
---
--- Nothing returned, but after necessary count of invokes `state.objs` will
--- hold list of resulting objects.
local function process_tuple(self, state, tuple, opts)
    local limit = opts.limit
    local filter = opts.filter
    local do_filter = opts.do_filter
    local pivot_filter = opts.pivot_filter
    local qcontext = state.qcontext

    local full_scan_cnt = opts.is_full_scan and opts.fetches_cnt or 0
    local index_lookup_cnt = opts.is_full_scan and 0 or opts.fetches_cnt
    qcontext.statistics:objects_fetched({
        fetches_cnt = opts.fetches_cnt,
        fetched_objects_cnt = opts.fetched_tuples_cnt,
        full_scan_cnt = full_scan_cnt,
        index_lookup_cnt = index_lookup_cnt,
    })

    qcontext.statistics:cache_lookup({
        cache_hits_cnt = opts.cache_hits_cnt,
        cache_hit_objects_cnt = opts.cache_hit_tuples_cnt,
    })

    check_deadline_clock(qcontext)

    local collection_name = opts.collection_name
    local pcre = opts.pcre
    local expr = opts.expr
    local resolveField = opts.resolveField
    local variables = qcontext.variables

    -- convert tuple -> object
    local obj = opts.unflatten_tuple(self, collection_name, tuple,
        { use_tomap = opts.use_tomap }, opts.default_unflatten_tuple)

    -- skip all items before pivot (the item pointed by offset)
    if not state.pivot_found and pivot_filter then
        local match = utils.is_subtable(obj, pivot_filter)
        if not match then return true end
        state.pivot_found = true
        return true -- skip pivot item too
    end

    -- make subrequests if needed
    local truncated_filter = table.copy(filter)
    for k, v in pairs(filter) do
        if obj[k] == nil then
            local field_name = k
            local sub_filter = v
            local field, is_list = resolveField(field_name, obj, sub_filter)
            if field == nil then return true end
            if is_list then
                if next(field) == nil then return true end
            end
            truncated_filter[k] = nil
        end
    end

    -- filter out non-matching objects
    local match = utils.is_subtable(obj, truncated_filter) and
        match_using_re(pcre, obj) and match_using_expr(expr, obj, variables)
    if do_filter then
        if not match then return true end
    else
        assert(match, 'found object do not fit passed filter: ' ..
            json.encode(obj))
    end

    -- add the matching object, update count and check limit
    state.objs[#state.objs + 1] = obj
    state.count = state.count + 1

    if not opts.is_hidden then
        qcontext.statistics:objects_retired({
            retired_objects_cnt = 1,
        })
    end

    if limit ~= nil and state.count >= limit then
        return false
    end
    return true
end

--- Call one of accessor function: `update_tuple` or `delete_tuple` for each
--- selected object.
---
--- @tparam table self data accessor instance
---
--- @tparam string collection_name
---
--- @tparam string schema_name
---
--- @tparam table selected objects to perform the operation
---
--- @tparam string operation 'update_tuple' or 'delete_tuple'
---
--- @param[opt] ... parameters to pass to the function
---
--- @treturn table `new_objects` list of returned objects (in the order of the
--- `selected` parameter)
local function perform_primary_key_operation(self, collection_name, schema_name,
        selected, operation, ...)
    check(operation, 'operation', 'string')

    local _, primary_index_meta = db_schema_helpers.get_primary_index_meta(
        self, collection_name)

    local new_objects = {}

    for _, object in ipairs(selected) do
        local key = {}
        for _, field_name in ipairs(primary_index_meta.fields) do
            table.insert(key, object[field_name])
        end
        -- XXX: we can pass a tuple corresponding the object to update_tuple /
        -- delete_tuple to save one get operation
        local new_tuple = self.funcs[operation](self, collection_name, key, ...)
        local new_object = self.funcs.unflatten_tuple(self, collection_name,
            new_tuple, {use_tomap = self.collection_use_tomap[collection_name]},
            self.default_unflatten_tuple[schema_name])
        table.insert(new_objects, new_object)
    end

    return new_objects
end

--- The function prepares context for tuples selection, postprocessing and
--- filtering.
---
--- @tparam table self the data accessor created by the `new` function
--- (directly or indirectly using the `accessor_space.new` or the
--- `accessor_shard.new` function)
---
--- @tparam string collection_name name of collection to perform select
---
--- @tparam table from collection and connection names we arrive from/by as
--- defined in the `impl.new` function description
---
--- @tparam table filter subset of object fields with values by which we want
--- to find full object(s)
---
--- @tparam table args table of arguments passed within the query except ones
--- that forms the `filter` parameter
---
--- @tparam table extra table which contains extra information related to
--- current select and the whole query
---
--- @treturn table `res` with `request_opts`, `select_state`, `select_opts` and
--- `args` fields
local function prepare_select_internal(self, collection_name, from, filter,
        args, extra)
    check(self, 'self', 'table')
    check(collection_name, 'collection_name', 'string')
    check(from, 'from', 'table')
    check(filter, 'filter', 'table')
    check(args, 'args', 'table')
    check(args.limit, 'args.limit', 'number', 'nil')
    -- XXX: save type of args.offset at parsing and check here
    -- check(args.offset, 'args.offset', ...)
    check(args.pcre, 'args.pcre', 'table', 'nil')
    check(extra.exp_tuple_count, 'extra.exp_tuple_count', 'number', 'nil')

    local collection = self.collections[collection_name]
    assert(collection ~= nil,
        ('cannot find the collection "%s"'):format(
        collection_name))
    assert(self.funcs.is_collection_exists(self, collection_name),
        ('cannot find collection "%s"'):format(collection_name))

    -- search for suitable index
    -- note: we redefine filter here
    local full_match, index_name, filter, index_value, pivot =
        self.index_finder:find(collection_name, from, filter, args)
    local index = index_name ~= nil and
        self.funcs.get_index(self, collection_name, index_name) or nil
    if index_name ~= nil and index == nil then
        error(('cannot find actual index "%s" in the collection "%s", ' ..
            'but the index metainformation contains it'):format(index_name,
            collection_name))
    end
    if from.collection_name ~= nil then
        -- allow fullscan only for a top-level object
        assert(index ~= nil,
            ('cannot find index "%s" in space "%s"'):format(
            index_name, collection_name))
    end

    -- lookup function for unflattening
    local schema_name = collection.schema_name
    assert(type(schema_name) == 'string',
        'schema_name must be a string, got ' .. type(schema_name))
    local default_unflatten_tuple = self.default_unflatten_tuple[schema_name]
    assert(default_unflatten_tuple ~= nil,
        ('cannot find default_unflatten_tuple for collection "%s"'):format(
        collection_name))

    -- read-write variables for process_tuple
    local qcontext = extra.qcontext
    local select_state = {
        count = 0,
        objs = {},
        pivot_found = false,
        qcontext = qcontext
    }

    -- compile an expression argument if provided and did not compiled yet
    local expr = args.filter
    check(expr, 'expression', 'table', 'string', 'nil')
    if type(expr) == 'string' then
        expr = expressions.new(expr)
    end

    -- propagate constants in the expression
    if expr ~= nil then
        expr = constant_propagation.transform(expr,
            {variables = qcontext.variables})
    end

    -- read only process_tuple options
    local select_opts = {
        limit = args.limit,
        filter = filter,
        do_filter = not full_match,
        pivot_filter = nil, -- filled later if needed
        collection_name = collection_name,
        unflatten_tuple = self.funcs.unflatten_tuple,
        use_tomap = self.collection_use_tomap[collection_name] or false,
        default_unflatten_tuple = default_unflatten_tuple,
        pcre = args.pcre,
        expr = expr,
        resolveField = extra.resolveField,
        is_hidden = extra.is_hidden,
    }

    -- assert that connection constraint applied only to objects got from the
    -- index that underlies the connection
    if extra.exp_tuple_count ~= nil then
        local err = 'internal error: connection constraint (expected tuple ' ..
            'count) cannot be applied to an index that is not under a ' ..
            'connection'
        assert(from.collection_name ~= nil, err)
        assert(index ~= nil, err)
        assert(pivot == nil or (pivot.value_list == nil and
            pivot.filter ~= nil), err)
    end

    local iterator_opts = {}
    local is_full_scan

    if index == nil then
        assert(pivot == nil,
            'offset for top-level objects must use a primary index')
        index = self.funcs.get_primary_index(self, collection_name)
        index_value = nil
        is_full_scan = true
    else
        if pivot ~= nil then
            -- handle case when there is pivot item (offset was passed)
            if pivot.value_list ~= nil then
                -- the 'fast offset' case
                assert(type(pivot.value_list) == 'table',
                    'pivot.value_list must be nil or a table, got ' ..
                    type(pivot.value_list))
                index_value = pivot.value_list
                iterator_opts.iterator = 'GT'
            elseif pivot.filter ~= nil then
                -- the 'slow offset' case
                assert(type(pivot.filter) == 'table',
                    'pivot.filter must be nil or a table, got ' ..
                    type(pivot.filter))
                select_opts.pivot_filter = pivot.filter
            else
                error('unexpected value of pivot: ' .. json.encode(pivot))
            end
        end

        is_full_scan = false
    end

    -- It is safe to pass limit down to the iterator when we do not filter
    -- objects after fetching. We do not lean on assumption that an iterator
    -- respects passed limit, so it is free (but unlikely is optimal) for an
    -- accessor to ignore it.
    if full_match and args.limit ~= nil then
        iterator_opts.limit = args.limit
    end

    -- request options can be changed below
    local request_opts = {
        index = index,
        index_name = index_name,
        index_value = index_value,
        iterator_opts = iterator_opts,
        is_full_scan = is_full_scan,
    }

    return {
        request_opts = request_opts,
        select_state = select_state,
        select_opts = select_opts,
        collection_name = collection_name,
        from = from,
        filter = filter,
        args = args,
        extra = extra,
    }
end

--- XXX
local function invoke_select_internal(self, prepared_select)
    local request_opts = prepared_select.request_opts
    local select_state = prepared_select.select_state
    local select_opts = prepared_select.select_opts
    local collection_name = prepared_select.collection_name
    local args = prepared_select.args
    local extra = prepared_select.extra

    local index = request_opts.index
    local index_name = request_opts.index_name
    local index_value = request_opts.index_value
    local iterator_opts = request_opts.iterator_opts
    local is_full_scan = request_opts.is_full_scan

    local tuple_count = 0
    local out = {}

    -- lookup for needed data in the cache if it is supported
    local iterable
    if self:cache_is_supported() then
        iterable = self.funcs.cache_lookup(self, collection_name, index_name,
            index_value, iterator_opts)
    end
    if iterable == nil then
        iterable = index
    end

    for _, tuple in iterable:pairs(index_value, iterator_opts, out) do
        local fetches_cnt = out.fetches_cnt or 0
        local fetched_tuples_cnt = out.fetched_tuples_cnt or 0
        local cache_hits_cnt = out.cache_hits_cnt or 0
        local cache_hit_tuples_cnt = out.cache_hit_tuples_cnt or 0
        check(fetches_cnt, 'fetches_cnt', 'number')
        check(fetched_tuples_cnt, 'fetched_tuples_cnt', 'number')
        check(cache_hits_cnt, 'cache_hits_cnt', 'number')
        check(cache_hit_tuples_cnt, 'cache_hit_tuples_cnt', 'number')
        out.fetches_cnt = 0
        out.fetched_tuples_cnt = 0
        out.cache_hits_cnt = 0
        out.cache_hit_tuples_cnt = 0

        tuple_count = tuple_count + 1

        -- check full match constraint
        if extra.exp_tuple_count ~= nil and
                tuple_count > extra.exp_tuple_count then
            error(('FULL MATCH constraint was failed: we got more then ' ..
                '%d tuples'):format(extra.exp_tuple_count))
        end

        select_opts.is_full_scan = is_full_scan
        select_opts.fetches_cnt = fetches_cnt
        select_opts.fetched_tuples_cnt = fetched_tuples_cnt
        select_opts.cache_hits_cnt = cache_hits_cnt
        select_opts.cache_hit_tuples_cnt = cache_hit_tuples_cnt
        local continue = process_tuple(self, select_state, tuple,
            select_opts)
        if not continue then break end
    end

    -- check full match constraint
    if extra.exp_tuple_count ~= nil and
            tuple_count ~= extra.exp_tuple_count then
        error(('FULL MATCH constraint was failed: we expect %d tuples, ' ..
            'got %d'):format(extra.exp_tuple_count, tuple_count))
    end

    local count = select_state.count
    local objs = select_state.objs

    assert(args.limit == nil or count <= args.limit,
        ('internal error: selected objects count (%d) exceeds limit (%s)')
            :format(count, args.limit))
    assert(#objs == count,
        ('internal error: selected objects count (%d) is not equal size of ' ..
            'selected object list (%d)'):format(count, #objs))

    return objs
end

--- Insert an object.
---
--- Parameters are the same as for @{prepare_select_internal}.
---
--- @treturn table list of a single object we inserted
---
--- We can just return the object and omit prepare_select_internal() /
--- invoke_select_internal() calls, because we forbid any filters/args that
--- could affect the result.
local function insert_internal(self, collection_name, from, filter, args, extra)
    local object = extra.extra_args.insert
    if object == nil then return nil end
    local err_msg = '"insert" must be the only argument when it is present'
    assert(next(filter) == nil, err_msg)
    assert(next(args) == nil, err_msg)
    assert(next(extra.extra_args, next(extra.extra_args)) == nil, err_msg)
    check(from, 'from', 'table')
    -- allow only top level collection
    check(from.collection_name, 'from.collection_name', 'nil')

    -- convert object -> tuple (set default values from a schema)
    local schema_name = db_schema_helpers.get_schema_name(self, collection_name)
    local default_flatten_object = self.default_flatten_object[schema_name]
    assert(default_flatten_object ~= nil,
        ('cannot find default_flatten_object ' ..
        'for collection "%s"'):format(collection_name))
    local tuple = self.funcs.flatten_object(self, collection_name, object, {
        service_fields_defaults =
            self.service_fields_defaults[schema_name],
    }, default_flatten_object)

    -- insert tuple & tuple -> object (with default values set before)
    local new_tuple = self.funcs.insert_tuple(self, collection_name, tuple)
    local use_tomap = self.collection_use_tomap[collection_name]
    local new_object = self.funcs.unflatten_tuple(self, collection_name,
        new_tuple, {use_tomap = use_tomap},
        self.default_unflatten_tuple[schema_name])

    return {new_object}
end

--- Update an object.
---
--- Same-named parameters meaning is the same as for
--- @{prepare_select_internal}.
---
--- @tparam table self the data accessor instance
---
--- @tparam string collection_name name of collection to perform update
---
--- @tparam table extra `extra.extra_args.update` is used
---
--- @tparam table selected objects to perform update
---
--- @treturn table `new_objects` list of updated objects (in the order of the
--- `selected` parameter)
local function update_internal(self, collection_name, extra, selected)
    local xobject = extra.extra_args.update
    if xobject == nil then return nil end

    local err_msg = '"update" must not be passed with "insert" or "delete" ' ..
        'arguments'
    assert(next(extra.extra_args, next(extra.extra_args)) == nil, err_msg)

    -- convert xobject -> update statements
    local schema_name = db_schema_helpers.get_schema_name(self, collection_name)
    local default_xflatten = self.default_xflatten[schema_name]
    assert(default_xflatten ~= nil,
        ('cannot find default_xflatten ' ..
        'for collection "%s"'):format(collection_name))
    local statements = self.funcs.xflatten(self, collection_name, xobject, {
        service_fields_defaults =
            self.service_fields_defaults[schema_name],
    }, default_xflatten)

    return perform_primary_key_operation(self, collection_name, schema_name,
        selected, 'update_tuple', statements)
end

--- Delete an object.
---
--- Corresponding parameters are the same as for @{prepare_select_internal}.
---
--- @tparam table self
---
--- @tparam string collection_name
---
--- @tparam table extra `extra.extra_args.delete` is used
---
--- @tparam table selected objects to delete
---
--- @treturn table `new_objects` list of deleted objects (in the order of the
--- `selected` parameter)
local function delete_internal(self, collection_name, extra, selected)
    if not extra.extra_args.delete then return nil end

    local err_msg = '"delete" must not be passed with "insert" or "update" ' ..
        'arguments'
    assert(next(extra.extra_args, next(extra.extra_args)) == nil, err_msg)

    local schema_name = db_schema_helpers.get_schema_name(self, collection_name)

    return perform_primary_key_operation(self, collection_name, schema_name,
        selected, 'delete_tuple')
end

--- Set of asserts to check the `funcs` argument of the @{accessor_general.new}
--- function.
--- @tparam table funcs set of function as defined in the
--- @{accessor_general.new} function decription
--- @return nil
local function validate_funcs(funcs)
    assert(type(funcs) == 'table',
        'funcs must be a table, got ' .. type(funcs))
    assert(type(funcs.is_collection_exists) == 'function',
        'funcs.is_collection_exists must be a function, got ' ..
        type(funcs.is_collection_exists))
    assert(type(funcs.get_index) == 'function',
        'funcs.get_index must be a function, got ' ..
        type(funcs.get_index))
    assert(type(funcs.get_primary_index) == 'function',
        'funcs.get_primary_index must be a function, got ' ..
        type(funcs.get_primary_index))
    assert(type(funcs.unflatten_tuple) == 'function',
        'funcs.unflatten_tuple must be a function, got ' ..
        type(funcs.unflatten_tuple))
    assert(type(funcs.flatten_object) == 'function',
        'funcs.flatten_object must be a function, got ' ..
        type(funcs.flatten_object))
    assert(type(funcs.xflatten) == 'function',
        'funcs.xflatten must be a function, got ' ..
        type(funcs.xflatten))
    assert(type(funcs.insert_tuple) == 'function',
        'funcs.insert_tuple must be a function, got ' ..
        type(funcs.insert_tuple))
    assert(type(funcs.update_tuple) == 'function',
        'funcs.update_tuple must be a function, got ' ..
        type(funcs.update_tuple))
    assert(type(funcs.delete_tuple) == 'function',
        'funcs.delete_tuple must be a function, got ' ..
        type(funcs.delete_tuple))
    check(funcs.cache_fetch, 'funcs.cache_fetch', 'function', 'nil')
    -- check(funcs.cache_delete, 'funcs.cache_delete', 'function', 'nil')
    check(funcs.cache_truncate, 'funcs.cache_truncate', 'function', 'nil')
    check(funcs.cache_lookup, 'funcs.cache_lookup', 'function', 'nil')
end

local function validate_query_settings(query_settings, opts)
    local opts = opts or {}
    local allow_nil = opts.allow_nil or false

    local resulting_object_cnt_max = query_settings.resulting_object_cnt_max
    local fetched_object_cnt_max = query_settings.fetched_object_cnt_max
    local timeout_ms = query_settings.timeout_ms

    if not allow_nil or type(resulting_object_cnt_max) ~= 'nil' then
        assert(type(resulting_object_cnt_max) == 'number' and
            resulting_object_cnt_max > 0,
            'resulting_object_cnt_max must be natural number')
    end
    if not allow_nil or type(fetched_object_cnt_max) ~= 'nil' then
        assert(type(fetched_object_cnt_max) == 'number' and
            fetched_object_cnt_max > 0,
            'fetched_object_cnt_max must be natural number')
    end
    if not allow_nil or type(timeout_ms) ~= 'nil' then
        assert(type(timeout_ms) == 'number' or (type(timeout_ms) == 'cdata' and
            tostring(ffi.typeof(timeout_ms)) == 'ctype<uint64_t>'),
            'timeout_ms must a number, got ' .. type(timeout_ms))
        assert(timeout_ms <= TIMEOUT_INFINITY,
            ('timeouts more then graphql.TIMEOUT_INFINITY (%s) ' ..
            'do not supported'):format(tostring(TIMEOUT_INFINITY)))
    end
end

--- This function is called on first select related to a query. Its purpose is
--- to initialize qcontext table.
--- @tparam table accessor
--- @tparam table qcontext per-query table which stores query internal state;
--- all neccessary initialization of this parameter should be performed by this
--  function
local function init_qcontext(accessor, qcontext)
    if qcontext.initialized then return end

    for k, v in pairs(accessor.query_settings_default) do
        if qcontext.query_settings[k] == nil then
            qcontext.query_settings[k] = v
        end
    end
    validate_query_settings(qcontext.query_settings)

    qcontext.deadline_clock = clock.monotonic64() +
        qcontext.query_settings.timeout_ms * 1000 * 1000

    local query_settings = qcontext.query_settings
    qcontext.statistics = statistics.new({
        resulting_object_cnt_max = query_settings.resulting_object_cnt_max,
        fetched_object_cnt_max = query_settings.fetched_object_cnt_max,
    })

    qcontext.initialized = true
end

--- Create default unflatten/flatten/xflatten functions, that can be called
--- from funcs.unflatten_tuple/funcs.flatten_object/funcs.xflatten when an
--- additional pre/postprocessing is not needed.
---
--- @tparam table models list of compiled schemas
---
--- @treturn table table with default_* fields with maps from schema name to
--- the corresponding default *flatten function
local function gen_default_object_tuple_map_funcs(models)
    local default_unflatten_tuple = {}
    local default_flatten_object = {}
    local default_xflatten = {}
    for schema_name, model in pairs(models) do
        default_unflatten_tuple[schema_name] = function(_, _, tuple, opts)
            local opts = opts or {}
            check(opts, 'opts', 'table')

            local ok, obj = model.unflatten(tuple)
            assert(ok, ('cannot unflat tuple of schema "%s": %s'):format(
                schema_name, tostring(obj)))
            return obj
        end
        default_flatten_object[schema_name] = function(_, _, obj, opts)
            local opts = opts or {}
            check(opts, 'opts', 'table')
            local sf_defaults = opts.service_fields_defaults or {}
            check(sf_defaults, 'service_fields_defaults', 'table')

            local ok, tuple = model.flatten(obj, unpack(sf_defaults))
            assert(ok, ('cannot flat object of schema "%s": %s'):format(
                schema_name, tostring(tuple)))
            return tuple
        end
        default_xflatten[schema_name] = function(_, _, xobject, opts)
            local opts = opts or {}
            check(opts, 'opts', 'table')

            -- it is not needed now, but maybe needed in custom xflatten
            local sf_defaults = opts.service_fields_defaults or {}
            check(sf_defaults, 'service_fields_defaults', 'table')

            local ok, statements = model.xflatten(xobject)
            assert(ok, ('cannot xflat xobject of schema "%s": %s'):format(
                schema_name, tostring(xobject)))
            return statements
        end
    end
    return {
        default_unflatten_tuple = default_unflatten_tuple,
        default_flatten_object = default_flatten_object,
        default_xflatten = default_xflatten,
    }
end

--- Create a new data accessor.
---
--- Provided `funcs` argument determines certain functions for retrieving
--- tuples.
---
--- @tparam table opts set of options:
---
--- * `schemas`,
--- * `collections`,
--- * `service_fields`,
--- * `indexes`,
--- * `collection_use_tomap`: ({[collection_name] = whether objects in
---    collection `collection_name` intended to be unflattened using
---    `tuple:tomap({names_only = true}` method instead of
---    `compiled_avro_schema.unflatten(tuple), ...}`),
--- * `resulting_object_cnt_max` and `fetched_object_cnt_max` are optional
---   positive numbers which help to control query behaviour in case it
---   requires more resources than expected _(default value is 10,000 for
---   both)_,
--- * `timeout_ms` _(default is 1000)_,
--- * `name` is 'space' or 'shard',
--- * `data_cache` (optional) is accessor_shard_cache instance.
---
--- For examples of `opts.schemas` and `opts.collections` consider the
--- @{impl.new} function description.
---
--- Example of `opts.service_fields` item:
---
---     service_fields['schema_name'] = {
---         {name = 'expires_on', type = 'long', default = 0},
---     }
---
--- Example of `opts.indexes` item:
---
---     indexes['collection_name'] = {
---         foo_bar = {
---             service_fields = {},
---             fields = {'foo', 'bar'},
---             unique = true | false, -- optional; used to validate connection
---                                    -- type if provided
---             primary = true | false, -- for now it is used only for 'offset'
---                                     -- argument processing, so it is more
---                                     -- or less optional; we do not validate
---                                     -- that there is an one primary index
---         },
---         ...
---     }
---
--- @tparam table funcs set of functions:
---
--- * `is_collection_exists`,
--- * `get_index`,
--- * `get_primary_index`,
--- * `unflatten_tuple`,
--- * `flatten_object`,
--- * `insert_tuple`,
--- * `cache_fetch` (optional),
--- -- * `cache_delete` (optional),
--- * `cache_truncate` (optional),
--- * `cache_lookup` (optional).
---
--- They allows this abstract data accessor behaves in the certain way (say,
--- like space data accessor or shard data accessor); consider the
--- `accessor_space` and the `accessor_shard` modules documentation for these
--- functions description.
---
--- @treturn table data accessor instance, a table with the methods as
--- described in the @{impl.new} function description.
---
--- Brief explanation of some select function parameters:
---
--- * `from` (table or nil) is nil for a top-level collection or a table with
---   the following fields:
---
---   - collection_name
---   - connection_name
---   - destination_args_names
---   - destination_args_values
---
--- * `extra` (table) is a table which contains additional data for the query:
---
---   - `qcontext` (table) can be used by an accessor to store any
---     query-related data;
---   - `resolveField(field_name, object, filter, opts)` (function) for
---     performing a subrequest on a fields connected using a connection.
---   - extra_args
---   - exp_tuple_count
function accessor_general.new(opts, funcs)
    check(opts, 'opts', 'table')
    check(funcs, 'funcs', 'table')

    local schemas = opts.schemas
    local collections = opts.collections
    local service_fields = opts.service_fields
    local indexes = opts.indexes

    check(schemas, 'schemas', 'table')
    check(collections, 'collections', 'table')
    check(service_fields, 'service_fields', 'table')
    check(indexes, 'indexes', 'table')

    local resulting_object_cnt_max = opts.resulting_object_cnt_max or
        DEF_RESULTING_OBJECT_CNT_MAX
    local fetched_object_cnt_max = opts.fetched_object_cnt_max or
        DEF_FETCHED_OBJECT_CNT_MAX
    local timeout_ms = opts.timeout_ms or DEF_TIMEOUT_MS
    local query_settings_default = {
        resulting_object_cnt_max = resulting_object_cnt_max,
        fetched_object_cnt_max = fetched_object_cnt_max,
        timeout_ms = timeout_ms,
    }
    validate_query_settings(query_settings_default)

    local name = opts.name
    local data_cache = opts.data_cache
    check(name, 'name', 'string')
    check(data_cache, 'data_cache', 'table', 'nil')

    local models, service_fields_defaults = compile_schemas(schemas,
        service_fields)
    validate_collections(collections, schemas, indexes)
    local index_finder = find_index.new(opts)
    local default_object_tuple_map_funcs =
        gen_default_object_tuple_map_funcs(models)

    validate_funcs(funcs)

    return setmetatable({
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
        models = models,
        default_unflatten_tuple =
            default_object_tuple_map_funcs.default_unflatten_tuple,
        default_flatten_object =
            default_object_tuple_map_funcs.default_flatten_object,
        default_xflatten =
            default_object_tuple_map_funcs.default_xflatten,
        service_fields_defaults = service_fields_defaults,
        collection_use_tomap = opts.collection_use_tomap or {},
        index_finder = index_finder,
        funcs = funcs,
        query_settings_default = query_settings_default,
        name = name,
        data_cache = data_cache,
    }, {
        __index = {
            select = function(self, parent, collection_name, from,
                    filter, args, extra)
                local inserted = insert_internal(self, collection_name, from,
                    filter, args, extra)
                if inserted ~= nil then return inserted end

                local prepared_select = self:prepare_select(parent,
                    collection_name, from, filter, args, extra)
                local selected = self:invoke_select(prepared_select)

                local updated = update_internal(self, collection_name, extra,
                    selected)
                if updated ~= nil then return updated end

                local deleted = delete_internal(self, collection_name, extra,
                    selected)
                if deleted ~= nil then return deleted end

                return selected
            end,
            prepare_select = function(self, parent, collection_name, from,
                        filter, args, extra)
                check(parent, 'parent', 'table')
                validate_from_parameter(from)

                init_qcontext(self, extra.qcontext)

                return prepare_select_internal(self, collection_name, from,
                    filter, args, extra)
            end,
            invoke_select = invoke_select_internal,
            cache_is_supported = function(self)
                return self.data_cache ~= nil
            end,
            cache_fetch = function(self, batches, qcontext)
                if not self:cache_is_supported() then
                    return nil
                end

                local res = self.funcs.cache_fetch(self, batches)
                if res == nil then
                    return nil
                end

                local fetch_id = res.fetch_id
                local stat = res.stat
                check(fetch_id, 'fetch_id', 'number')
                check(stat, 'stat', 'table')
                check(stat.fetches_cnt, 'stat.fetches_cnt', 'number')
                check(stat.fetched_tuples_cnt, 'stat.fetched_tuples_cnt',
                    'number')
                check(stat.full_scan_cnt, 'stat.full_scan_cnt', 'number')
                check(stat.index_lookup_cnt, 'stat.index_lookup_cnt', 'number')

                -- update statistics
                init_qcontext(self, qcontext)
                qcontext.statistics:objects_fetched({
                    fetches_cnt = stat.fetches_cnt,
                    fetched_objects_cnt = stat.fetched_tuples_cnt,
                    full_scan_cnt = stat.full_scan_cnt,
                    index_lookup_cnt = stat.index_lookup_cnt
                })

                check_deadline_clock(qcontext)

                return fetch_id
            end,
            -- Unused for now.
            -- cache_delete = function(self, fetch_id)
            --     if not self:cache_is_supported() then
            --         return
            --     end
            --     self.funcs.cache_delete(self, fetch_id)
            -- end,
            cache_truncate = function(self)
                if not self:cache_is_supported() then
                    return
                end
                self.funcs.cache_truncate(self)
            end,
        }
    })
end

-- export the function
accessor_general.validate_query_settings = validate_query_settings

return accessor_general

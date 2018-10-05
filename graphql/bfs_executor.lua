--- Breadth-first executor with support of batching of similar requests.
---
--- ## The execution loop
---
--- The execution loop comprises the series of two main steps: preparing
--- get/select requests for a data accessor and actual performing of this
--- requests. The executor traverses the query tree in the breadth-first order,
--- so it keeps the list of requests prepared on the previous iteration called
--- `open_set`.
---
--- The iteration starts with extracting a prepared request (this data
--- structure called `prepared_object`) from `open_set`, then the executor
--- resolves it (performs the actual data access request using
--- `prepared_resolve:invoke()`), filters needed fields (inside
--- `filter_object()`) and 'opens' child nodes with preparing of further
--- requests (forming `fields_info` structure in `filter_object()`). The
--- prepared requests then added to the end of `open_set` to be processed on
--- the next iterations.
---
--- The filtering of an object (`filter_object()`) performs deep filtering of
--- inner fields which are requested by a user in a query (see
--- `filter_value()`). This auxiliary traversal does not involves a data
--- accessor calls and made in depth-first order.
---
--- Additional steps are made to minimize actual count of network requests,
--- they are described in the following sections.
---
--- ## Batching similar requests
---
--- The executor performs analyzing of prepared requests (`open_set`) and forms
--- so called 'batches' to pull the results of similar data accessor requests
--- within one network request. This is why the prepared request stage is
--- needed. The similar requests are ones which have the same collection, the
--- same index and the same iterator options (say, 'GT'), but different keys.
--- The analysing is performed for each level of the traversed tree, so
--- `squash_marker` item is added to the `open_set` to distinguish one level of
--- the following one.
---
--- The analyzing generates 'batches' (in `fetch_first_same()`) and passes it
--- to the `fetch()` function of `accessor_shard_cache` instance, which
--- performs the requests using a stored procedure on storage servers and saves
--- the results into the cache. Then the executor continue the loop as usual.
--- Data access requests which will be performed inside
--- `prepared_resolve:invoke()` will use the cached data (when `accessor_shard`
--- is used). The cached data are cleared at the end of the query execution
--- (because of cache only requests described later).
---
--- This approach reduces overall count of network requests (but makes they
--- heavier), so the time of stalls on awaiting of network requests is reduced
--- too. The economy is more when round trip time (delay to the first byte of a
--- request result) is larger.
---
--- The analyzing and the prefetching are performed separately for so called
--- `prepared_object_list` structure (in `fetch_resolve_list()`), because
--- `prepared_object`s from one 'list' request are processed inside one
--- iteration of the execution loop (in `invoke_resolve_list()`). This is the
--- tech debt and hopefully will be changed in the future.
---
--- ## Filtering over a connection
---
--- Normally a request fetches objects from one collection and the prefetching
--- effectively reduces count of network requests. But there is the special
--- kind of requests that involves fetching of objects from another
--- collections. They are requests with an argument named as a connection with
--- semantic 'select such objects from the current collection for which the
--- connected object matches the argument'.
---
--- The resolving function (`prepared_resolve:invoke()`) of a prepared request
--- performs such auxiliary requests itself, but we need to prefetch the
--- connected objects to still effectively use the network.
---
--- The executor generates so called cache only prepared requests which are
--- needed only to analyze and prefetch needed objects from connected
--- collections using `fetch_first_same()` function. Such requests are added to
--- the separate queue `cache_only_open_set` which have precedence over
--- the `open_set` one. So needed objects will be prefetched in a batch and
--- cached before the 'real' request will try to access it.
---
--- The important detail here is that we need an actual object to fetch its
--- connected object. So the executor generates a request without
--- 'connection arguments' for the current object and place it to
--- `cache_only_open_set`. The request is formed in such way that leads to deep
--- fetching of its connected objects on further iterations of the execution
--- loop (see `generate_cache_only_request()`). The child requests of a cache
--- only request are added to the end of the `cache_only_open_set` queue.
---
--- The cache only requests are the reason why we cannot clear the data cache
--- at end of the processing of the current tree level: the executor going down
--- by the tree with cache only requests and then continue processing the
--- current level with 'real' requests. This is why the cached data are cleared
--- at end of the query execution.
---
--- ## Data structures
---
--- The main data structure of this module is the list of requests to be
--- processed:
---
---     open_set = {
---         prepared_object_list = <list of prepared_object>,
---         ... or ...
---         prepared_object = {
---             filtered_object = <...>,
---             fields_info = {
---                 [field_name] = {
---                     is_list = <boolean>
---                     kind = <...>,
---                     prepared_resolve = {
---                         is_calculated = true,
---                         objs = <...>,
---                         ... or ...
---                         is_calculated = false,
---                         prepared_select = {
---                             request_opts = {
---                                 index = <iterator>,
---                                 index_name = <string or nil>,
---                                 index_value = <key>,
---                                 iterator_opts = <...>,
---                                 is_full_scan = <boolean>,
---                             }
---                             select_state = {
---                                 count = <number>,
---                                 objs = <list of objects>,
---                                 pivot_found = <boolean>,
---                                 qcontext = <table>,
---                             },
---                             select_opts = {
---                                 model = <compiled avro schema>,
---                                 limit = <number>,
---                                 filter = <table>,
---                                 do_filter = <boolean>,
---                                 pivot_filter = <table>,
---                                 resolveField = <function>,
---                                 is_hidden = <boolean>,
---                             },
---                             collection_name = <string>,
---                             from = <...>,
---                             filter = <...>,
---                             args = <...>,
---                             extra = {
---                                 qcontext = <table>,
---                                 resolveField = <function>, -- for
---                                                            -- subrequests
---                                 extra_args = <table>,
---                                 exp_tuple_count = <number or nil>,
---                             },
---                         },
---                         accessor = <...>,
---                         connection = <...>,
---                         invoke = <function>,
---                     },
---                     selections = {
---                         {
---                             name = {
---                                 value = <string>, -- field name
---                             },
---                             kind = 'field',
---                             selectionSet = {
---                                 selections = <...>,
---                             },
---                             arguments = {
---                                 {
---                                     name = {
---                                         value = <string>, -- argument name
---                                     },
---                                     value = <...>,
---                                 },
---                                 ...
---                             },
---                             coerced_arguments = <...>,
---                         },
---                         ...
---                     }
---                 },
---                 ...
---             }
---         }
---     }

local utils = require('graphql.utils')
local core_util = require('graphql.core.util')
local core_types = require('graphql.core.types')
local core_query_util = require('graphql.core.query_util')
local core_validate_variables = require('graphql.core.validate_variables')
local core_introspection = require('graphql.core.introspection')
local request_batch = require('graphql.request_batch')

-- XXX: Possible cache_only requests refactoring. Maybe just set
-- `is_cache_only` flag in prepared_object, find such request in within the
-- execution loop and perform before others. This can heavily simplify the
-- code, because allows to avoid separate `cache_only_*` structures, but
-- involves extra `open_set` scan.

-- XXX: Possible singleton/list requests refactoring. We can add
-- `prepared_object`s of a list request to `open_set` as separate items and
-- work with them as with singleton ones. It allows to remove
-- `fetch_resolve_list` function and just use `fetch_first_same`, but requires
-- to 'open' all list requests up to the marker to saturate
-- `cache_only_open_set` and only then switch between cache_only / plain
-- requests (make it on the marker).
--
-- Maybe we also can remove separate `is_list = true` case processing over the
-- code that can havily simplify things.

-- XXX: It would be more natural to have list of tables with field_info content
-- + field_name instead of fields_info to handle each prepared resolve
-- separatelly and add ability to reorder it.

local bfs_executor = {}

-- forward declarations
local fetch_first_same
local fetch_resolve_list

-- Generate cache only requests for filters over connections {{{

--- Convert per-connection filters to selections (recursively).
local function filters_to_selections(bare_object_type, filters)
    local selections = {}
    for k, v in pairs(filters) do
        local field_type = bare_object_type.fields[k]
        assert(field_type ~= nil, 'field_type must not be nil')
        if field_type.prepare_resolve ~= nil then
            -- per-connection field
            assert(type(v) == 'table',
                'per-connection field filter must be a table, got ' .. type(v))

            local new_selection = {
                name = {
                    value = k
                },
                kind = 'field',
                -- selectionSet is nil
                -- coerced_arguments is nil
            }
            table.insert(selections, new_selection)

            local bare_inner_type = core_types.bare(field_type.kind)
            local child_selections = filters_to_selections(bare_inner_type, v)
            if next(child_selections) ~= nil then
                new_selection.selectionSet = {}
                new_selection.selectionSet.selections = child_selections
            end

            -- add arguments on local collection fields
            for child_k, child_v in pairs(v) do
                local is_sel_found = false
                for _, sel in ipairs(child_selections) do
                    if sel.name.value == child_k then
                        is_sel_found = true
                    end
                end
                if not is_sel_found then
                    if new_selection.coerced_arguments == nil then
                        new_selection.coerced_arguments = {}
                    end
                    assert(new_selection.coerced_arguments[child_k] == nil,
                        'found two selections with the same name')
                    new_selection.coerced_arguments[child_k] = child_v
                end
            end
        end
    end
    return selections
end

--- Generate a cache only request for a filter over a connection.
local function generate_cache_only_request(prepared_resolve, field_type,
        object, is_list, args, info)
    if prepared_resolve.is_calculated then
        return nil
    end

    local inner_type = field_type.kind
    local bare_inner_type = core_types.bare(inner_type)
    local connection_filters = {}
    local local_args = {}

    local filter = prepared_resolve.prepared_select.filter
    for k, v in pairs(filter) do
        local connection_field_type = bare_inner_type.fields[k]
        assert(connection_field_type ~= nil,
            'internal error: connection_field_type should not be nil')
        local is_connection_field =
            connection_field_type.prepare_resolve ~= nil
        if is_connection_field then
            connection_filters[k] = v
        end
    end

    for k, v in pairs(args) do
        if connection_filters[k] == nil then
            local_args[k] = v
        end
    end

    -- create cache only requests for requests with 'by connection' filters and
    -- without extra arguments (like mutation arguments)
    local extra_args = prepared_resolve.prepared_select.extra.extra_args or {}
    if next(connection_filters) == nil or next(extra_args) ~= nil then
        return nil
    end

    -- cache_only_prepared_resolve is the same as prepared_resolve, but it does
    -- not require fetching connected objects to fetch and return this one
    local cache_only_prepared_resolve = field_type.prepare_resolve(
        object, local_args, info, {is_hidden = true})
    local cache_only_selections = filters_to_selections(
        bare_inner_type, filter)
    return {
        is_list = is_list,
        kind = inner_type,
        prepared_resolve = cache_only_prepared_resolve,
        selections = cache_only_selections,
    }
end

-- }}}

-- Filter object / create prepared resolve {{{

local function get_argument_values(field_type, selection, variables)
    local args = {}
    for argument_name, value in pairs(selection.coerced_arguments or {}) do
        args[argument_name] = value
    end
    for _, argument in ipairs(selection.arguments or {}) do
        local argument_name = argument.name.value
        assert(argument_name ~= nil, 'argument_name must not be nil')
        local argument_type = field_type.arguments[argument_name]
        assert(argument_type ~= nil,
            ('cannot find argument "%s"'):format(argument_name))
        local value = core_util.coerceValue(argument.value, argument_type,
            variables, {strict_non_null = true})
        args[argument_name] = value
    end
    return args
end

local function evaluate_selections(object_type, selections, context)
    assert(object_type.__type == 'Object')

    local selections_per_fields = {}

    local fields = core_query_util.collectFields(object_type, selections, {},
        {}, context)

    for _, field in ipairs(fields) do
        assert(selections_per_fields[field.name] == nil,
            'two selections into the one field: ' .. field.name)
        assert(field.selection ~= nil)
        selections_per_fields[field.name] = field.selection
    end

    return selections_per_fields
end

--- Select fields from an object value, preprocess an other value
--- appropriately.
local function filter_value(value, value_type, selections, context)
    if value_type.__type == 'NonNull' then
        if value == nil then
            error('No value provided for non-null ' ..
                (value_type.name or value_type.__type))
        end
        value_type = core_types.nullable(value_type)
    end

    if value == nil then
        return nil
    end

    if value_type.__type == 'Scalar' or value_type.__type == 'Enum' then
        return value_type.serialize(value)
    elseif value_type.__type == 'List' then
        local child_type = value_type.ofType
        assert(child_type ~= nil)
        assert(type(value) == 'table')
        assert(utils.is_array(value))

        local res = {}
        for _, child_value in ipairs(value) do
            table.insert(res, filter_value(child_value, child_type, selections,
                context))
        end

        return res
    elseif value_type.__type == 'Object' then
        -- note: the code is pretty same as filter_object, but forbid
        -- prepare_resolve attribute (because we have no such nested objects)
        -- and avoid construction of fields_info and similar structures
        assert(type(value) == 'table')
        local selections_per_fields = evaluate_selections(value_type,
            selections, context)
        local res = {}

        for field_name, selection in pairs(selections_per_fields) do
            local field_type = core_introspection.fieldMap[field_name] or
                value_type.fields[field_name]
            assert(field_type ~= nil)
            assert(field_type.prepare_resolve == nil,
                'resolving inside nested records in not supported')

            local child_value = value[field_name]
            local child_type = field_type.kind

            if field_type.resolve ~= nil then
                local info = {
                    schema = context.schema,
                    parentObject = value_type,
                }
                -- args parameter is always empty list
                child_value = field_type.resolve(value, {}, info)
            end

            local child_selections = selection.selectionSet ~= nil and
                selection.selectionSet.selections or {}

            assert(res[field_name] == nil)
            res[field_name] = filter_value(child_value, child_type,
                child_selections, context)
        end

        return res
    elseif value_type.__type == 'Union' then
        local resolved_type = value_type.resolveType(value)
        return filter_value(value, resolved_type, selections, context)
    end

    error('Unknown type: ' .. tostring(value_type.__type))
end

--- Select fields from fetched object and create prepared resolve functions for
--- connection fields.
---
--- @tparam table object (can be nil)
---
--- @tparam table object_type GraphQL type
---
--- @tparam table selections structure describing fields should be shown in the
--- query result and arguments to pass to these fields
---
--- @tparam table context the following structure:
---
---     {
---         schema = schema,
---         variables = variables,
---         fragmentMap = fragmentMap,
---     }
---
--- @tparam table qcontext the following options:
---
--- * is_item_cache_only
--- * qcontext: query-local storage for various purposes
---
--- @treturn table `prepared` of the following format:
---
---     prepared = {
--          cache_only_prepared_object = <...>,
--          prepared_object = <...>,
---     }
---
--- `cache_only_prepared_object` and `prepared_object` has the following
--- structure:
---
---    [cache_only_]prepared_object = {
---        filtered_object = <...fields from <object>...>,
---        fields_info =
---            [field_name] = {
---                is_list = <boolean>,
---                kind = <...>,
---                prepared_resolve = <...>,
---                selections = <...>,
---            },
---            ...
---        }
---    }
local function filter_object(object, object_type, selections, context, opts)
    local opts = opts or {}
    local qcontext = opts.qcontext
    local is_item_cache_only = opts.is_item_cache_only or false

    local nullable_object_type = core_types.nullable(object_type)

    if object_type.__type == 'NonNull' then
        if object == nil then
            error('No value provided for non-null ' ..
                (nullable_object_type.name or nullable_object_type.__type))
        end
        object_type = nullable_object_type
    end

    if object == nil then
        return {
            -- cache_only_prepared_object is nil
            -- prepared_object is nil
        }
    end

    assert(object_type.__type == 'Object')

    local selections_per_fields = evaluate_selections(object_type, selections,
        context)

    local filtered_object = {}
    local cache_only_fields_info = {}
    local fields_info = {}

    for field_name, selection in pairs(selections_per_fields) do
        local field_type = core_introspection.fieldMap[field_name] or
            object_type.fields[field_name]
        assert(field_type ~= nil)
        local child_selections = selection.selectionSet ~= nil and
            selection.selectionSet.selections or {}

        local inner_type = field_type.kind
        local nullable_inner_type = core_types.nullable(inner_type)
        local is_list = nullable_inner_type.__type == 'List'

        local args
        local info

        if field_type.prepare_resolve ~= nil or field_type.resolve ~= nil then
            args = get_argument_values(field_type, selection, context.variables)
            info = {
                qcontext = qcontext,
                schema = context.schema,
                parentObject = object_type,
            }
        end

        if field_type.prepare_resolve then
            local prepared_resolve = field_type.prepare_resolve(object, args,
                info, {is_hidden = is_item_cache_only})

            fields_info[field_name] = {
                is_list = is_list,
                kind = inner_type,
                prepared_resolve = prepared_resolve,
                selections = child_selections,
            }

            local cache_only_fields_info_item =
                generate_cache_only_request(prepared_resolve, field_type,
                    object, is_list, args, info)
            if cache_only_fields_info_item ~= nil then
                cache_only_fields_info[field_name] = cache_only_fields_info_item
            end
        else
            local value = object[field_name]
            if field_type.resolve ~= nil then
                value = field_type.resolve(object, args, info)
            end
            assert(filtered_object[field_name] == nil)
            filtered_object[field_name] = filter_value(
                value, inner_type, child_selections, context)
        end
    end

    local cache_only_prepared_object
    if next(cache_only_fields_info) ~= nil then
        cache_only_prepared_object = {
            filtered_object = filtered_object,
            fields_info = cache_only_fields_info,
        }
    end
    local prepared_object = {
        filtered_object = filtered_object,
        fields_info = fields_info,
    }
    return {
        cache_only_prepared_object = cache_only_prepared_object,
        prepared_object = prepared_object,
    }
end

local function filter_object_list(object_list, object_type, selections, context,
        opts)
    local opts = opts or {}
    local qcontext = opts.qcontext
    local is_item_cache_only = opts.is_item_cache_only or false

    local nullable_object_type = core_types.nullable(object_type)
    assert(nullable_object_type.__type == 'List')

    if object_type.__type == 'NonNull' then
        if object_list == nil then
            error('No value provided for non-null ' ..
                (nullable_object_type.name or nullable_object_type.__type))
        end
        object_type = nullable_object_type
    end

    assert(object_type.__type == 'List')
    object_type = object_type.ofType
    assert(object_type ~= nil)

    local prepared_object_list = {}
    local cache_only_prepared_object_list = {}

    if object_list == nil then
        object_list = nil -- box.NULL -> nil
    end

    for _, object in ipairs(object_list or {}) do
        local prepared = filter_object(object, object_type, selections, context,
            {qcontext = qcontext, is_item_cache_only = is_item_cache_only})
        local cache_only_prepared_object = prepared.cache_only_prepared_object
        local prepared_object = prepared.prepared_object
        table.insert(cache_only_prepared_object_list,
            cache_only_prepared_object)
        table.insert(prepared_object_list, prepared_object)
    end

    return {
        prepared_object_list = prepared_object_list,
        cache_only_prepared_object_list = cache_only_prepared_object_list,
    }
end

-- }}}

-- Resolve prepared requests and call object filtering {{{

local function invoke_resolve(prepared_object, context, opts)
    local opts = opts or {}
    local qcontext = opts.qcontext
    local is_item_cache_only = opts.is_item_cache_only or false

    local cache_only_open_set = {}
    local open_set = {}

    for field_name, field_info in pairs(prepared_object.fields_info) do
        local object_or_list
        local object_type
        if field_info.prepared_resolve.is_calculated then
            object_or_list = field_info.prepared_resolve.objs
        else
            object_or_list, object_type = field_info.prepared_resolve:invoke()
        end
        object_type = object_type or field_info.kind
        local selections = field_info.selections

        local child_cache_only
        local child

        if field_info.is_list then
            local child_prepared_list = filter_object_list(
                object_or_list, object_type, selections, context,
                {qcontext = qcontext, is_item_cache_only = is_item_cache_only})
            -- don't perform construction for cache_only objects
            local child_cache_only_prepared_object_list =
                child_prepared_list.cache_only_prepared_object_list
            local child_prepared_object_list =
                child_prepared_list.prepared_object_list

            -- construction
            if not is_item_cache_only then
                prepared_object.filtered_object[field_name] = {}
                for _, child_prepared_object in
                        ipairs(child_prepared_object_list) do
                    table.insert(prepared_object.filtered_object[field_name],
                        child_prepared_object.filtered_object)
                end
            end

            if next(child_prepared_object_list) ~= nil then
                child = {
                    prepared_object_list = child_prepared_object_list,
                }
            end

            if next(child_cache_only_prepared_object_list) ~= nil then
                child_cache_only = {
                    prepared_object_list = child_cache_only_prepared_object_list,
                }
            end
        else
            local child_prepared = filter_object(object_or_list,
                object_type, selections, context, {qcontext = qcontext,
                is_item_cache_only = is_item_cache_only})
            -- don't perform construction for cache_only objects
            local child_cache_only_prepared_object =
                child_prepared.cache_only_prepared_object
            local child_prepared_object = child_prepared.prepared_object

            if child_prepared_object ~= nil then
                -- construction
                if not is_item_cache_only then
                    prepared_object.filtered_object[field_name] =
                        child_prepared_object.filtered_object
                end

                child = {
                    prepared_object = child_prepared_object,
                }
            end

            if child_cache_only_prepared_object ~= nil then
                child_cache_only = {
                    prepared_object = child_cache_only_prepared_object,
                }
            end
        end

        -- add to cache_only_open_set when we catch the object from it
        if is_item_cache_only then
            table.insert(cache_only_open_set, child)
        else
            table.insert(open_set, child)
        end
        table.insert(cache_only_open_set, child_cache_only)
    end

    return {
        cache_only_open_set = cache_only_open_set,
        open_set = open_set,
    }
end

local function invoke_resolve_list(prepared_object_list, context, opts)
    local opts = opts or {}
    local qcontext = opts.qcontext
    local accessor = opts.accessor
    local is_item_cache_only = opts.is_item_cache_only or false
    local max_batch_size = opts.max_batch_size

    local open_set = {}
    local cache_only_open_set = {}

    local last_fetched_object_num = 0
    for i, prepared_object in ipairs(prepared_object_list) do
        if i > last_fetched_object_num then
            local _, size = fetch_resolve_list(prepared_object_list,
                {accessor = accessor, qcontext = qcontext,
                max_batch_size = max_batch_size, start_from = i,
                force_caching = is_item_cache_only})
            last_fetched_object_num = last_fetched_object_num + size
        end

        local child = invoke_resolve(prepared_object, context,
            {qcontext = qcontext, is_item_cache_only = is_item_cache_only})
        local child_open_set = child.open_set
        local child_cache_only_open_set = child.cache_only_open_set

        utils.expand_list(open_set, child_open_set)
        utils.expand_list(cache_only_open_set, child_cache_only_open_set)
    end

    return {
        open_set = open_set,
        cache_only_open_set = cache_only_open_set,
    }
end

-- }}}

-- Analyze prepared requests and prefetch in batches {{{

fetch_first_same = function(open_set, opts)
    local func_name = 'bfs_executor.fetch_first_same'
    local opts = opts or {}
    local accessor = opts.accessor
    local qcontext = opts.qcontext
    local max_batch_size = opts.max_batch_size
    local force_caching = opts.force_caching or false

    if not accessor:cache_is_supported() then return nil, 0 end

    local size = 0

    local batches = {}
    for i, item in ipairs(open_set) do
        if i > max_batch_size then break end
        if item.prepared_object == nil then break end
        local prepared_object = item.prepared_object

        for field_name, field_info in pairs(prepared_object.fields_info) do
            local prepared_resolve = field_info.prepared_resolve
            if prepared_resolve.is_calculated then
                size = i
                goto ret
            end
            local batch = request_batch.from_prepared_resolve(prepared_resolve)

            if i == 1 then
                assert(batches[field_name] == nil,
                    ('internal error: %s: field names "%s" clash'):format(
                    func_name, field_name))
                batches[field_name] = batch
                size = i
            else
                local ok = batches[field_name] ~= nil and
                    batches[field_name]:compare_bins(batch)
                if not ok then goto ret end
                table.insert(batches[field_name].keys, batch.keys[1])
                size = i
            end
        end
    end

    ::ret::

    -- don't flood cache with single-key (non-batch) select results
    if not force_caching and size <= 1 then
        return nil, size
    end

    local fetch_id = accessor:cache_fetch(batches, qcontext)
    return fetch_id, size
end

fetch_resolve_list = function(prepared_object_list, opts)
    local func_name = 'bfs_executor.fetch_resolve_list'
    local opts = opts or {}
    local accessor = opts.accessor
    local qcontext = opts.qcontext
    local max_batch_size = opts.max_batch_size
    local start_from = opts.start_from or 1
    local force_caching = opts.force_caching or false

    if not accessor:cache_is_supported() then return nil, 0 end

    local size = 0

    local batches = {}
    for i = 1, #prepared_object_list - start_from + 1 do
        if i > max_batch_size then break end
        local prepared_object = prepared_object_list[i + start_from - 1]

        for field_name, field_info in pairs(prepared_object.fields_info) do
            local prepared_resolve = field_info.prepared_resolve
            if prepared_resolve.is_calculated then
                size = i
                goto ret
            end
            local batch = request_batch.from_prepared_resolve(prepared_resolve)

            if i == 1 then
                assert(batches[field_name] == nil,
                    ('internal error: %s: field names "%s" clash'):format(
                    func_name, field_name))
                batches[field_name] = batch
                size = i
            else
                local ok, err = batches[field_name]:compare_bins_extra(batch)
                if not ok then
                    error(('internal error: %s: %s'):format(func_name, err))
                end
                table.insert(batches[field_name].keys, batch.keys[1])
                size = i
            end
        end
    end

    ::ret::

    -- don't flood cache with single-key (non-batch) select results
    if not force_caching and size <= 1 then
        return nil, size
    end

    local fetch_id = accessor:cache_fetch(batches, qcontext)
    return fetch_id, size
end

-- }}}

-- Reorder requests before add to open_set {{{

local function expand_open_set(open_set, child_open_set, opts)
    local opts = opts or {}
    local accessor = opts.accessor

    if not accessor:cache_is_supported() then
        utils.expand_list(open_set, child_open_set)
        return
    end

    local item_bin_to_ordinal = {}
    local items_per_ordinal = {}
    local next_ordinal = 1

    -- Create histogram-like 'items_per_ordinal' structure with lists of items.
    -- Each list contain items of the same kind (with the same bin value).
    -- Ordinals of the bins are assigned in order of appear in child_open_set.
    for _, item in ipairs(child_open_set) do
        if item.prepared_object_list ~= nil then
            local ordinal = next_ordinal
            assert(items_per_ordinal[ordinal] == nil)
            items_per_ordinal[ordinal] = {}
            next_ordinal = next_ordinal + 1
            table.insert(items_per_ordinal[ordinal], item)
        else
            local prepared_object = item.prepared_object
            assert(prepared_object ~= nil)
            assert(prepared_object.fields_info ~= nil)

            local batch_bins = {}
            for field_name, field_info in pairs(prepared_object.fields_info) do
                local prepared_resolve = field_info.prepared_resolve
                if prepared_resolve.is_calculated then
                    table.insert(batch_bins, field_name .. ':<calculated>')
                else
                    local batch = request_batch.from_prepared_resolve(
                        prepared_resolve)
                    table.insert(batch_bins, field_name .. ':' .. batch:bin())
                end
            end

            local item_bin = table.concat(batch_bins, ';')
            local ordinal = item_bin_to_ordinal[item_bin]
            if ordinal == nil then
                item_bin_to_ordinal[item_bin] = next_ordinal
                ordinal = next_ordinal
                assert(items_per_ordinal[ordinal] == nil)
                items_per_ordinal[ordinal] = {}
                next_ordinal = next_ordinal + 1
            end
            table.insert(items_per_ordinal[ordinal], item)
        end
    end

    -- add items from child_open_set in ordinals order to open_set
    for _, items in ipairs(items_per_ordinal) do
        utils.expand_list(open_set, items)
    end
end

-- }}}

-- Debugging {{{

local function prepared_object_digest(prepared_object)
    local json = require('json')
    local digest = {
        ['='] = prepared_object.filtered_object,
    }
    for k, v in pairs(prepared_object.fields_info) do
        if v.prepared_resolve.is_calculated then
            digest[k] = '<calculated>'
        else
            local prepared_select = v.prepared_resolve.prepared_select
            local collection_name = prepared_select.collection_name
            local request_opts = prepared_select.request_opts
            local key = request_opts.index_value or box.NULL
            local filter = prepared_select.filter
            digest[k] = {
                c = collection_name,
                k = key,
                f = filter,
            }
        end
    end
    return json.encode(digest)
end

local function open_set_tostring(open_set, name)
    local res = ('\n==== %s ====\n'):format(name)
    for _, item in ipairs(open_set) do
        if item.prepared_object ~= nil then
            local digest = prepared_object_digest(item.prepared_object)
            res = res .. '\nprepared_object: ' .. digest
        elseif item.prepared_object_list ~= nil then
            res = res .. '\nprepared_object_list:'
            for _, prepared_object in ipairs(item.prepared_object_list) do
                local digest = prepared_object_digest(prepared_object)
                res = res .. '\n    ' .. digest
            end
        elseif item.squash_marker ~= nil then
            if item.fetch_id ~= nil then
                res = res .. '\nsquash marker: ' .. tostring(item.fetch_id)
            else
                res = res .. '\nsquash marker'
            end
        else
            res = res .. '\nunknown open_set item'
        end
    end
    return res
end

-- }}}

-- The main execution loop {{{

--- Execute a GraphQL query.
---
--- @tparam table schema
---
--- @tparam table query_ast
---
--- @tparam table variables
---
--- @tparam string operation_name
---
--- @tparam table opts the following options:
---
--- * qcontext
--- * accessor
--- * max_batch_size
---
--- @treturn table result of the query
function bfs_executor.execute(schema, query_ast, variables, operation_name, opts)
    local opts = opts or {}
    local qcontext = opts.qcontext
    local accessor = opts.accessor
    local max_batch_size = opts.max_batch_size

    local operation = core_query_util.getOperation(query_ast, operation_name)
    local root_object_type = schema[operation.operation]
    assert(root_object_type ~= nil,
        ('cannot find root type for operation "%s"'):format(operation_name))
    local root_selections = operation.selectionSet.selections

    local fragmentMap = core_query_util.getFragmentDefinitions(query_ast)
    local context = {
        schema = schema,
        variables = variables,
        fragmentMap = fragmentMap,
    }

    local root_object = {}

    -- validate variables
    local variableTypes = core_query_util.getVariableTypes(schema, operation)
    core_validate_variables.validate_variables({
        variables = variables,
        variableTypes = variableTypes,
    })

    local prepared_root = filter_object(
        root_object, root_object_type, root_selections, context,
        {qcontext = qcontext})
    local prepared_root_object = prepared_root.prepared_object
    local cache_only_prepared_root_object =
        prepared_root.cache_only_prepared_object
    local filtered_root_object = prepared_root_object.filtered_object

    local cache_only_open_set = {}
    if cache_only_prepared_root_object ~= nil then
        table.insert(cache_only_open_set, {
            prepared_object = cache_only_prepared_root_object
        })
    end

    local open_set = {}
    if prepared_root_object ~= nil then
        table.insert(open_set, {
            prepared_object = prepared_root_object
        })
    end

    table.insert(cache_only_open_set, 1, {squash_marker = true})
    table.insert(open_set, 1, {squash_marker = true})

    while true do
        -- don't perform cache only requests if cache is not supported by the
        -- accessor
        if not accessor:cache_is_supported() then
            cache_only_open_set = {}
        end

        utils.debug(open_set_tostring, cache_only_open_set,
            'cache only open set')
        utils.debug(open_set_tostring, open_set, 'open set')

        local item
        local is_item_cache_only = next(cache_only_open_set) ~= nil
        if is_item_cache_only then
            item = table.remove(cache_only_open_set, 1)
        else
            item = table.remove(open_set, 1)
        end

        utils.debug(open_set_tostring, {item}, 'item (before)')

        if item == nil then break end
        if item.prepared_object ~= nil then
            local child = invoke_resolve(item.prepared_object, context,
                {qcontext = qcontext,
                is_item_cache_only = is_item_cache_only})
            local child_cache_only_open_set = child.cache_only_open_set
            local child_open_set = child.open_set
            expand_open_set(cache_only_open_set, child_cache_only_open_set,
                {accessor = accessor})
            expand_open_set(open_set, child_open_set, {accessor = accessor})
        elseif item.prepared_object_list ~= nil then
            local child = invoke_resolve_list(item.prepared_object_list,
                context, {qcontext = qcontext, accessor = accessor,
                is_item_cache_only = is_item_cache_only,
                max_batch_size = max_batch_size})
            local child_cache_only_open_set = child.cache_only_open_set
            local child_open_set = child.open_set
            expand_open_set(cache_only_open_set, child_cache_only_open_set,
                {accessor = accessor})
            expand_open_set(open_set, child_open_set, {accessor = accessor})
        elseif item.squash_marker ~= nil then
            local open_set_to_fetch = is_item_cache_only and
                cache_only_open_set or open_set
            local fetch_id, size = fetch_first_same(open_set_to_fetch,
                {accessor = accessor, qcontext = qcontext,
                max_batch_size = max_batch_size,
                force_caching = is_item_cache_only})
            if #open_set_to_fetch > 0 then
                table.insert(open_set_to_fetch, math.max(2, size + 1), {
                    squash_marker = true,
                    fetch_id = fetch_id,
                })
            end
        else
            assert(false, 'unknown open_set item format')
        end

        utils.debug(open_set_tostring, {item}, 'item (after)')
    end

    accessor:cache_truncate()

    return filtered_root_object
end

-- }}}

return bfs_executor

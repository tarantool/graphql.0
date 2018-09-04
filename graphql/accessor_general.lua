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
local bit = require('bit')
local rex, is_pcre2 = utils.optional_require_rex()
local avro_helpers = require('graphql.avro_helpers')
local db_schema_helpers = require('graphql.db_schema_helpers')
local error_codes = require('graphql.error_codes')
local statistics = require('graphql.statistics')

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

--- Get a key to lookup index by `lookup_index_name` (part of `index_cache`).
---
--- @tparam table filter filter for objects, its keys (names of fields) will
--- form the result of the function
---
--- @treturn string `name_list_str` (key for lookup by `lookup_index_name`)
local function filter_names_fingerprint(filter)
    local name_list = {}
    local function fill_name_list(filter, base_name)
        for field_name, v in pairs(filter) do
            if type(v) == 'table' then
                fill_name_list(v, base_name .. field_name .. '.')
            else
                name_list[#name_list + 1] = base_name .. field_name
            end
        end
    end
    fill_name_list(filter, '')
    table.sort(name_list)
    local name_list_str = table.concat(name_list, ',')
    return name_list_str
end

--- Get an index using parts tree built by @{build_index_parts_tree}.
---
--- @tparam table node root of the prefix tree for certain collection
---
--- @tparam table filter map of key-value to filter objects against
---
--- @treturn string `index_name` or `nil` is found index
---
--- @treturn number `max_branch_len` is a number of index parts will be used at
--- lookup plus 1 (because it calculated artificial root node as well as other
--- nodes)
---
--- Complexity
--- ----------
---
--- In short: O(SIZE(filter)^2 * COUNT(index parts for all indexes)).
---
--- Say we have N fields in filter (N = SIZE(filter), M indexes and K index
--- parts at max ((M * K) and COUNT(index parts for all indexes) both are are
--- upside limits of nodes count in the tree). We look for successors for
--- each filter item (<= N items) in each of the tree node (<= M * K nodes),
--- so have O(I * N * (M * K)) of somewhat we call 'iteration' of I
--- complexity. Most heavy operation within an iteraton is table.copy(), we
--- can assume it has O(N) complexity. So we have overall complexity O(N^2 *
--- (M * K)).
---
--- We can consider worst case scenario when any node has any of filter keys as
--- a successor. In this case nodes count is not real constraint for recursion.
--- In such case we can calculate complexity as iteration of weight I
--- (calculated above as O(N^2)) and iteration count as permutations of N
--- filter items (N!). In such case we'll have O(N^2 * N!) or O(N^(3/2) * N^N)
--- (Stirling's approximation).
---
--- Expectations
--- ------------
---
--- We expect typical filter size as 1 or 2 and tree depth (excluding
--- artificial root node) of the same order. So despite horrible complexity
--- estimation it expected to be non-so-heavy. Our guess is that it worth to
--- try hard to find best index before a large request.
---
--- Future optimizations
--- --------------------
---
--- * replace table.copy() with something more light: maybe 'closed set' of
--    filter items or {remove filter[k], invoke the function, add
---   back filter[k]} (but it needed to be done in such way that will not
---   invalidate pairs());
--- * cache index name btw block requests of the same collection request (when
---   we'll have block executor) and maybe even btw different requests with the
--    same filter keys.
local function get_best_matched_index(node, filter)
    local index_name = (node.index_names or {})[1]
    local max_branch_len = 1

    -- optimization: don't run the loop below if there are no successors of the
    -- current node
    if node.successors == nil then
        return index_name, 1
    end

    for k, v in pairs(filter) do
        local successor_node = (node.successors or {})[k]
        if successor_node ~= nil then
            local new_filter = table.copy(filter)
            new_filter[k] = nil
            local branch_index_name, branch_len =
                get_best_matched_index(successor_node, new_filter)
            branch_len = branch_len + 1
            if branch_index_name ~= nil and branch_len > max_branch_len then
                index_name = branch_index_name
                max_branch_len = branch_len
            end
        end
    end

    return index_name, max_branch_len
end

-- XXX: raw idea: we can store field-to-field_no mapping when creating
-- `lookup_index_name` to faster form the value_list

--- Flatten filter values (transform to a list) against specific index to
--- passing it to index:pairs().
---
--- @tparam table self the data accessor
---
--- @tparam table filter filter for objects, its values will ordered to form
--- the result
---
--- @tparam string collection_name name of collection contains the index with
--- a name `index_name`
---
--- @tparam string index_name name of index against which `filter` values will
--- be ordered
---
--- @treturn boolean `full_match` whether the passed filter forms full key for
--- passed index
---
--- @treturn table `value_list` the value to pass to index:pairs()
---
--- @treturn table `new_filter` the `filter` value w/o values extracted to
--- `value_list`
local function flatten_filter(self, filter, collection_name, index_name)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))
    assert(type(index_name) == 'string',
        'index_name must be a string, got ' .. type(index_name))

    local value_list = {}
    local new_filter = table.copy(filter)

    -- fill value_list
    local index_meta = self.indexes[collection_name][index_name]
    -- XXX: support or remove indexes by service_fields
    assert(#index_meta.service_fields == 0,
        'service_fields support does not implemented yet')
    for _, field_name in ipairs(index_meta.fields) do
        local value = filter[field_name]
        if value == nil then break end
        value_list[#value_list + 1] = value
        new_filter[field_name] = nil
    end

    -- check for correctness: non-empty value_list
    if #value_list == 0 then -- avoid extra json.encode()
        assert(#value_list > 0,
            ('empty index key: filter: %s, index_name: %s'):format(
            json.encode(filter), index_name))
    end

    local full_match = #value_list == #index_meta.fields and
        next(new_filter) == nil
    return full_match, value_list, new_filter
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

--- Choose an index for lookup tuple(s) by a 'filter'. The filter holds fields
--- values of object(s) we want to find. It uses prebuilt `lookup_index_name`
--- table representing available indexes, which created by the
--- `build_lookup_index_name` function.
---
--- @tparam table self the data accessor created by the `new` function
--- (directly or indirectly using the `accessor_space.new` or the
--- `accessor_shard.new` function); this function uses the
--- `self.index_cache` prebuild table representing available indexes
---
--- @tparam string collection_name name of a collection of whose indexes the
--- function will search through
---
--- @tparam table from information about a connection bring executor to select
--- from a current collection; `from.collection_name == nil` means selecting
--- top-level objects; it has the following structure:
---
---     {
---         collection_name = <...> (string),
---         connection_name = <...> (string),
---         destination_args_names = <...> (list, lua table),
---         destination_args_values = <...> (list, lua table),
---     }
---
--- @tparam table filter map from fields names to values; names are used for
--- lookup needed index, values forms the `value_list` return value
---
--- @tparam table args the `args` argument from the `self:select()` function,
--- it is the `list_args_instance` variable in terms of the `convert_schema`
--- module; here we using only `args.offset` value
---
--- @treturn boolean `full_match` is whether passing `value_list` to the index
--- with name `index_name` will give tuple(s) proven to match the filter or
--- just some subset of all tuples in the collection which need to be filtered
--- further
---
--- @treturn string `index_name` is name of the found index or nil
---
--- @treturn table `new_filter` is the filter value w/o values extracted into
--- `value_list`
---
--- @treturn table `value_list` (optional) is values list from the `filter`
--- argument ordered in the such way that it can be passed to the found index
-- (has some meaning only when `index_name ~= nil`)
---
--- @treturn table `pivot` (optional) an offset argument represented depending
--- of a case: whether we'll lookup for the offset by an index; it is either
--- `nil`, or contains `value_list` field to pass to a GT (great-then) index,
--- or contains `filter` field to use in `process_tuple` for find the pivot in
--- a select result
local function get_index_name(self, collection_name, from, filter, args)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' .. type(collection_name))
    check(from, 'from', 'table')
    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))
    assert(type(args) == 'table',
        'args must be a table, got ' .. type(args))

    local index_cache = self.index_cache
    assert(type(index_cache) == 'table',
        'index_cache must be a table, got ' .. type(index_cache))

    local lookup_index_name = index_cache.lookup_index_name
    assert(type(lookup_index_name) == 'table',
        'lookup_index_name must be a table, got ' .. type(lookup_index_name))

    local parts_tree = index_cache.parts_tree
    assert(type(parts_tree) == 'table',
        'parts_tree must be a table, got ' .. type(parts_tree))

    local connection_indexes = index_cache.connection_indexes
    assert(type(connection_indexes) == 'table',
        'connection_indexes must be a table, got ' .. type(connection_indexes))

    -- That is the 'slow offset' case. Here we fetch objects by a connection.
    -- If an offset is set we return it as `pivot.filter`. So, select will be
    -- performed by an index from the connection, then the result will be
    -- postprocessed using `pivot`.
    if from.collection_name ~= nil then
        local connection_index =
            connection_indexes[collection_name][from.connection_name]
        local index_name = connection_index.index_name
        local connection_type = connection_index.connection_type
        assert(index_name ~= nil, 'index_name must not be nil')
        assert(connection_type ~= nil, 'connection_type must not be nil')
        local full_match = connection_type == '1:1' and next(filter) == nil
        local value_list = from.destination_args_values
        local new_filter = filter

        local pivot
        if args.offset ~= nil then
            local _, index_meta = db_schema_helpers.get_primary_index_meta(self,
                collection_name)
            local pivot_filter
            if #index_meta.fields == 1 then
                -- we use simple type in case of scalar offset
                local field_name = index_meta.fields[1]
                pivot_filter = {[field_name] = args.offset}
            else
                for _, field_name in ipairs(index_meta.fields) do
                    assert(args.offset[field_name] ~= nil,
                        ('offset by a partial key is forbidden: ' ..
                        'expected "%s" field'):format(field_name))
                    pivot_filter = {[field_name] = args.offset[field_name]}
                end
            end
            pivot = {filter = pivot_filter}
        end

        return full_match, index_name, new_filter, value_list, pivot
    end

    -- The 'fast offset' case. Here we fetch top-level objects starting from
    -- passed offset. Select will be performed by the primary index and
    -- corresponding offset in `pivot.value_list`, then the result will be
    -- postprocessed using `new_filter`, if necessary.
    if args.offset ~= nil then
        local index_name, index_meta = db_schema_helpers.get_primary_index_meta(
            self, collection_name)
        local full_match
        local pivot_value_list
        local new_filter = filter
        if type(args.offset) == 'table' then
            full_match, pivot_value_list, new_filter = flatten_filter(self,
                args.offset, collection_name, index_name)
            assert(full_match == true, 'offset by a partial key is forbidden')
        else
            assert(#index_meta.fields == 1,
                ('index parts count is not 1 for scalar offset: ' ..
                'index "%s"'):format(index_name))
            full_match, pivot_value_list = true, {args.offset}
        end
        local pivot = {value_list = pivot_value_list}
        full_match = full_match and next(filter) == nil
        return full_match, index_name, new_filter, nil, pivot
    end

    -- The 'no offset' case. Here we fetch top-level object either by found
    -- index or using full scan (if the index was not found).

    -- try to find full index
    local name_list_str = filter_names_fingerprint(filter)
    assert(lookup_index_name[collection_name] ~= nil,
        ('cannot find any index for collection "%s"'):format(collection_name))
    local index_name = lookup_index_name[collection_name][name_list_str]
    local full_match = false
    local value_list = nil
    local new_filter = filter

    -- try to find partial index
    if index_name == nil then
        local root = parts_tree[collection_name]
        index_name = get_best_matched_index(root, filter)
    end

    -- fill full_match and value_list appropriatelly
    if index_name ~= nil then
        full_match, value_list, new_filter = flatten_filter(self, filter,
            collection_name, index_name)
    end

    return full_match, index_name, new_filter, value_list
end

--- Build `lookup_index_name` table (part of `index_cache`) to use in the
--- @{get_index_name} function.
---
--- @tparam table indexes map from collection names to indexes as defined in
--- the @{new} function
---
--- @treturn table `lookup_index_name`
local function build_lookup_index_name(indexes)
    assert(type(indexes) == 'table', 'indexes must be a table, got ' ..
        type(indexes))
    local lookup_index_name = {}
    for collection_name, index in pairs(indexes) do
        assert(type(collection_name) == 'string',
            'collection_name must be a string, got ' .. type(collection_name))
        assert(type(index) == 'table',
            'index must be a table, got ' .. type(index))

        lookup_index_name[collection_name] = {}

        for index_name, index_descr in pairs(index) do
            assert(type(index_name) == 'string',
                'index_name must be a string, got ' .. type(index_name))
            assert(type(index_descr) == 'table',
                'index_descr must be a table, got ' .. type(index_descr))
            assert(type(index_descr.service_fields) == 'table',
                'index_descr.service_fields must be a table, got ' ..
                type(index_descr.service_fields))
            assert(type(index_descr.fields) == 'table',
                'index_descr.fields must be a table, got ' ..
                type(index_descr.fields))

            -- XXX: support or remove indexes by service_fields
            assert(#index_descr.service_fields == 0,
                'service_fields support does not implemented yet')

            local service_fields_list = index_descr['service_fields']
            assert(utils.is_array(service_fields_list),
                'service_fields_list must be an array')
            local fields_list = index_descr['fields']
            assert(utils.is_array(fields_list),
                'fields_list must be an array')
            assert(#service_fields_list + #fields_list > 0,
                'bad "indexes" parameter: no fields found')

            local name_list = {}
            for _, field in ipairs(service_fields_list) do
                assert(type(field) == 'string',
                    'field must be a string, got ' .. type(field))
                name_list[#name_list + 1] = field
            end
            for _, field in ipairs(fields_list) do
                assert(type(field) == 'string',
                    'field must be a string, got ' .. type(field))
                name_list[#name_list + 1] = field
            end

            table.sort(name_list)
            local name_list_str = table.concat(name_list, ',')
            lookup_index_name[collection_name][name_list_str] = index_name
        end
    end
    return lookup_index_name
end

--- Build `parts_tree` to use in @{get_index_name} for lookup best matching
--- index.
---
--- @tparam table indexes indexes metainformation as defined in the @{new}
--- function
---
--- Schetch example:
---
--- * collection_1:
---   * index 1 parts: foo, bar, baz;
---   * index 2 parts: foo, abc;
---   * index 3 parts: abc, efg, hij;
--    * index 4 parts: abc.
---
--- Resulting table of prefix trees (contains one field for collection_1):
---
--- ```
--- * collection_1:
---  \
---   + --> root node --> foo --> bar --> baz ~~> index 1
---          \             \
---           \             + --> abc ~~> index 2
---            \
---             + ------> abc --> efg --> hij ~~ index 3
---                        \
---                         + ~~> index 4
--- ```
---
--- @treturn table `roots` resulting table of prefix trees
---
--- * `roots` is a table which maps `collection names` to `root nodes` of
--- prefix trees;
--- * 'collection name` is a string (name of a collection);
--- * `root node` is a table with `successors` field;
--- * `successors` field value is a map from `index part` to `non-root node`;
--- * `index part` is a string (name of corresponding field in an object);
--- * `non-root node` is a table with `index_names` field and optional
---   `successors` field;
--- * `index_names` field value is a list of `index name`;
--- * `index name` is a string (name of an index).
local function build_index_parts_tree(indexes)
    local roots = {}

    for collection_name, indexes_meta in pairs(indexes) do
        local root = {}
        roots[collection_name] = root
        for index_name, index_meta in pairs(indexes_meta) do
            local cur = root
            for _, field in ipairs(index_meta.fields) do
                cur.successors = cur.successors or {}
                cur.successors[field] = cur.successors[field] or {}
                cur = cur.successors[field]
                cur.index_names = cur.index_names or {}
                cur.index_names[#cur.index_names + 1] = index_name
            end
        end
    end

    return roots
end

local function set_connection_index(c, c_name, c_type, collection_name,
                                    indexes, connection_indexes)
    assert(type(c.index_name) == 'string',
        'index_name must be a string, got ' .. type(c.index_name))

    -- validate index_name against 'indexes'
    local index_meta = indexes[c.destination_collection]
    assert(type(index_meta) == 'table',
        'index_meta must be a table, got ' .. type(index_meta))

    assert(type(collection_name) == 'string', 'collection_name expected to ' ..
        'be string, got ' .. type(collection_name))

    -- validate connection parts are match or being prefix of index
    -- fields
    local i = 1
    local index_fields = index_meta[c.index_name].fields
    for _, part in ipairs(c.parts) do
        assert(type(part.source_field) == 'string',
            'part.source_field must be a string, got ' ..
            type(part.source_field))
        assert(type(part.destination_field) == 'string',
            'part.destination_field must be a string, got ' ..
            type(part.destination_field))
        assert(part.destination_field == index_fields[i],
            ('connection "%s" of collection "%s" has destination parts that ' ..
            'is not prefix of the index "%s" parts ' ..
            '(destination collection - "%s")'):format(c_name, collection_name,
            c.index_name, c.destination_collection))
        i = i + 1
    end
    local parts_cnt = i - 1

    -- partial index of an unique index is not guaranteed to being
    -- unique
    assert(c_type == '1:N' or parts_cnt == #index_fields,
        ('1:1 connection "%s" of collection "%s" ' ..
        'has less fields than the index of "%s" collection ' ..
        '(cannot prove uniqueness of the partial index)'):format(c_name,
        collection_name, c.index_name, c.destination_collection))

    -- validate connection type against index uniqueness (if provided)
    if index_meta.unique ~= nil then
        assert(c_type == '1:N' or index_meta.unique == true,
            ('1:1 connection ("%s") cannot be implemented ' ..
            'on top of non-unique index ("%s")'):format(
            c_name, c.index_name))
    end

    return {
        index_name = c.index_name,
        connection_type = c_type,
    }
end

--- Build `connection_indexes` table (part of `index_cache`) to use in the
--- @{get_index_name} function.
---
--- @tparam table indexes map from collection names to indexes meta-information
--- as defined in the @{new} function; the function uses it to validate index
--- names provided in connections (which are inside collections), validate
--- connection types ('1:1' or '1:N') against index uniqueness if the `unique`
--- flag provided for corresponding index and to check that destination parts
--- of connections form a prefix of parts of the `connection.index_name` index
---
--- @tparam table collections map from collection names to collections as
--- defined in the @{accessor_general.new} function decription; the function
--- uses it to extract index names from connections and create the resulting
--- mapping
---
--- @treturn table `connection_indexes`
local function build_connection_indexes(indexes, collections)
    assert(type(indexes) == 'table', 'indexes must be a table, got ' ..
        type(indexes))
    assert(type(collections) == 'table', 'collections must be a table, got ' ..
        type(collections))
    local connection_indexes = {}
    for collection_name, collection in pairs(collections) do
        for _, c in ipairs(collection.connections) do
            if c.destination_collection ~= nil then
                if connection_indexes[c.destination_collection] == nil then
                    connection_indexes[c.destination_collection] = {}
                end

                connection_indexes[c.destination_collection][c.name] =
                set_connection_index(c, c.name, c.type, collection_name,
                    indexes, connection_indexes)
            end

            if c.variants ~= nil then
                for _, v in ipairs(c.variants) do
                    if connection_indexes[v.destination_collection] == nil then
                        connection_indexes[v.destination_collection] = {}
                    end
                    connection_indexes[v.destination_collection][c.name] =
                        set_connection_index(v, c.name, c.type, collection_name,
                            indexes, connection_indexes)
                end
            end
        end
    end
    return connection_indexes
end

--- General function that build connection and index information to use in the
--- @{get_index_name} function.
---
--- It uses the @{build_lookup_index_name} and the @{build_connection_indexes}
--- functions.
local function build_index_cache(indexes, collections)
    return {
        lookup_index_name = build_lookup_index_name(indexes),
        parts_tree = build_index_parts_tree(indexes),
        connection_indexes = build_connection_indexes(indexes, collections),
    }
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

--- Whether an object match set of PCRE.
---
--- @tparam table obj an object to check
---
--- @tparam table pcre map with PCRE as values; names are correspond to field
--- names of the `obj` to match
---
--- @treturn boolean `res` whether the `obj` object match `pcre` set of
--- regexps.
local function match_using_re(obj, pcre)
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
            local match = match_using_re(obj[field_name], re)
            if not match then return false end
        else
            local flags = rex.flags()
            -- emulate behaviour of (?i) on libpcre (libpcre2 supports it)
            local cfg = 0
            if not is_pcre2 then
                local cnt
                re, cnt = re:gsub('^%(%?i%)', '')
                if cnt > 0 then
                    cfg = bit.bor(cfg, flags.CASELESS)
                end
            end
            -- enable UTF-8
            if is_pcre2 then
                cfg = bit.bor(cfg, flags.UTF)
                cfg = bit.bor(cfg, flags.UCP)
            else
                cfg = bit.bor(cfg, flags.UTF8)
                cfg = bit.bor(cfg, flags.UCP)
            end
            -- XXX: compile re once
            local re = rex.new(re, cfg)
            if not re:match(obj[field_name]) then
                return false
            end
        end
    end

    return true
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
--- core of the `select_internal` function.
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
    local resolveField = opts.resolveField

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
        match_using_re(obj, pcre)
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
    local full_match, index_name, filter, index_value, pivot = get_index_name(
        self, collection_name, from, filter, args) -- we redefine filter here
    local index = index_name ~= nil and
        self.funcs.get_index(self, collection_name, index_name) or nil
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

    local iterator_opts = nil
    local is_full_scan

    if index == nil then
        assert(pivot == nil,
            'offset for top-level objects must use a primary index')
        index = self.funcs.get_primary_index(self, collection_name)
        index_value = nil
        is_full_scan = true
    else
        iterator_opts = iterator_opts or {}
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

        -- It is safe to pass limit down to the iterator when we do not filter
        -- objects after fetching. We do not lean on assumption that an
        -- iterator respects passed limit.
        -- Note: accessor_space does not support limit args (we need to wrap
        -- index:pairs() for that), but accessor_shard does (because calls
        -- index:select() under hood)
        if full_match and args.limit ~= nil then
            iterator_opts.limit = args.limit
        end

        is_full_scan = false
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
--- Parameters are the same as for @{select_internal}.
---
--- @treturn table list of a single object we inserted
---
--- We can just return the object and omit select_internal() call, because we
--- forbid any filters/args that could affect the result.
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
--- Same-named parameters meaning is the same as for @{select_internal}.
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
--- Corresponding parameters are the same as for @{select_internal}.
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
--- * `enable_mutations`: boolean flag _(default is `false` for avro-schema-2*
---    and `true` for avro-schema-3*)_,
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
    assert(type(opts) == 'table',
        'opts must be a table, got ' .. type(opts))
    assert(type(funcs) == 'table',
        'funcs must be a table, got ' .. type(funcs))

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

    -- Mutations are disabled for avro-schema-2*, because it can work
    -- incorrectly for schemas with nullable types.
    local enable_mutations
    if opts.enable_mutations == nil then
        enable_mutations = avro_helpers.major_avro_schema_version() == 3
    else
        enable_mutations = opts.enable_mutations
    end
    check(enable_mutations, 'enable_mutations', 'boolean')

    local name = opts.name
    local data_cache = opts.data_cache
    check(name, 'name', 'string')
    check(data_cache, 'data_cache', 'table', 'nil')

    local models, service_fields_defaults = compile_schemas(schemas,
        service_fields)
    validate_collections(collections, schemas, indexes)
    local index_cache = build_index_cache(indexes, collections)
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
        index_cache = index_cache,
        funcs = funcs,
        settings = {
            enable_mutations = enable_mutations,
        },
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

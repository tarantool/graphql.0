--- Find suitable index.

local json = require('json')
local utils = require('graphql.utils')
local db_schema_helpers = require('graphql.db_schema_helpers')

local check = utils.check

local find_index = {}

-- Build structures helpers {{{

--- Validate a connection against index set as underlying.
---
--- @tparam table connection
---
--- @tparam string collection_name
---
--- @tparam table indexes
---
--- @treturn nothing
local function validate_connection_vs_index(connection, collection_name, indexes)
    check(connection, 'connection', 'table')
    check(collection_name, 'collection_name', 'string')
    check(indexes, 'indexes', 'table')

    local c = connection

    check(c.index_name, 'index_name', 'string')

    -- validate index_name against 'indexes'
    local index_meta = indexes[c.destination_collection]
    check(index_meta, 'index_meta', 'table')

    -- validate connection parts are match or being prefix of index
    -- fields
    local i = 1
    local index_fields = index_meta[c.index_name].fields
    for _, part in ipairs(c.parts) do
        check(part.source_field, 'part.source_field', 'string')
        check(part.destination_field, 'part.destination_field', 'string')
        assert(part.destination_field == index_fields[i],
            ('connection "%s" of collection "%s" has destination parts that ' ..
            'is not prefix of the index "%s" parts ' ..
            '(destination collection - "%s")'):format(c.name, collection_name,
            c.index_name, c.destination_collection))
        i = i + 1
    end
    local parts_cnt = i - 1

    -- partial index of an unique index is not guaranteed to being
    -- unique
    assert(c.type == '1:N' or parts_cnt == #index_fields,
        ('1:1 connection "%s" of collection "%s" ' ..
        'has less fields than the index of "%s" collection ' ..
        '(cannot prove uniqueness of the partial index)'):format(c.name,
        collection_name, c.index_name, c.destination_collection))

    -- validate connection type against index uniqueness (if provided)
    if index_meta.unique ~= nil then
        assert(c.type == '1:N' or index_meta.unique == true,
            ('1:1 connection ("%s") cannot be implemented ' ..
            'on top of non-unique index ("%s")'):format(
            c.name, c.index_name))
    end
end

-- }}}

-- Find index helpers {{{

--- Whether list arguments can reduce resulting objects set.
---
--- The idea is the following: 'limit', empty 'pcre' and indexed 'offset'
--- arguments don't reduce the set of resulting objects, so should not prevent
--- us from setting full_match variable. Hovewer if other arguments are present
--- we should set full_match to false.
---
--- Note: we don't distinguish indexed (fast) and non-indexed (slow) offset
--- cases here, so a caller should handle that itself.
---
--- Note: extra_args now have mutation related arguments and nothing related to
--- selecting objects, so here we don't pay attention whether any extra
--- arguments provided or not. If this will be changed in the future, this
--- function should get extra_args as the argument and analyze it too.
---
--- @tparam table args list arguments
---
--- @treturn boolean
local function are_list_args_can_reduce_result(args)
    for k, v in pairs(args) do
        if k ~= 'limit' and k ~= 'pcre' and k ~= 'offset' then
            return true
        elseif k == 'pcre' and next(v) ~= nil then
            return true
        end
    end
    return false
end

-- XXX: raw idea: we can store field-to-field_no mapping when creating
-- `lookup_index_name` to faster form the value_list

--- Flatten filter values (transform to a list) against specific index to
--- passing it to index:pairs().
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
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
local function flatten_filter(db_schema, filter, collection_name, index_name)
    check(db_schema, 'db_schema', 'table')
    check(filter, 'filter', 'table')
    check(index_name, 'index_name', 'string')

    local value_list = {}
    local new_filter = table.copy(filter)

    -- fill value_list
    local index_meta = db_schema.indexes[collection_name][index_name]
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

--- Get a key to lookup index by `lookup_index_name` (part of
--- `index_cache_data`).
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

-- }}}

-- Build necessary structures {{{

--- Build `lookup_index_name` table (part of `index_cache_data`) to use in the
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
---             + ------> abc --> efg --> hij ~~> index 3
---                        \
---                         + ~~> index 4
--- ```
---
--- @treturn table `roots` resulting table of prefix trees
---
--- * `roots` is a table which maps `collection names` to `root nodes` of
---   prefix trees;
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

--- Build `connection_indexes` table (part of `index_cache_data`) to use in the
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
    check(indexes, 'indexes', 'table')
    check(collections, 'collections', 'table')
    local connection_indexes = {}
    for collection_name, collection in pairs(collections) do
        for _, c in ipairs(collection.connections) do
            local ok = (c.destination_collection ~= nil and c.variants == nil)
                or (c.destination_collection == nil and c.variants ~= nil)
            if not ok then
                error(('wrong connection format: either ' ..
                    '"destination_collection" or "variants" should be ' ..
                    'present, but not both: %s'):format(json.encode(c)))
            end

            if c.destination_collection ~= nil then
                if connection_indexes[c.destination_collection] == nil then
                    connection_indexes[c.destination_collection] = {}
                end
                validate_connection_vs_index(c, collection_name, indexes)
                connection_indexes[c.destination_collection][c.name] = {
                    index_name = c.index_name,
                    connection_type = c.type,
                }
            end

            if c.variants ~= nil then
                for _, v in ipairs(c.variants) do
                    if connection_indexes[v.destination_collection] == nil then
                        connection_indexes[v.destination_collection] = {}
                    end
                    local quazi_connection = {
                        type = c.type,
                        parts = v.parts,
                        name = c.name,
                        destination_collection = v.destination_collection,
                        index_name = v.index_name,
                    }
                    validate_connection_vs_index(quazi_connection,
                        collection_name, indexes)
                    connection_indexes[v.destination_collection][c.name] = {
                        index_name = v.index_name,
                        connection_type = c.type,
                    }
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

-- }}}

-- Find suitable index {{{

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
--- parts at max ((M * K) and COUNT(index parts for all indexes) both are
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

--- Choose an index for lookup tuple(s) by a 'filter'. The filter holds fields
--- values of object(s) we want to find. It uses prebuilt `lookup_index_name`
--- table representing available indexes, which created by the
--- `build_lookup_index_name` function.
---
--- @tparam table self the index finder instance
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
--- @tparam table args the `args` argument from the
--- @{accessor_general.prepare_select_internal} function, it is the
--- `list_args_instance` variable in terms of the `convert_schema` module
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
--- (has some meaning only when `index_name ~= nil`)
---
--- @treturn table `pivot` (optional) an offset argument represented depending
--- of a case: whether we'll lookup for the offset by an index; it is either
--- `nil`, or contains `value_list` field to pass to a GT (great-then) index,
--- or contains `filter` field to use in `process_tuple` for find the pivot in
--- a select result
local function get_index_name(self, collection_name, from, filter, args)
    check(self, 'self', 'table')
    check(collection_name, 'collection_name', 'string')
    check(from, 'from', 'table')
    check(filter, 'filter', 'table')
    check(args, 'args', 'table')

    local index_cache_data = self.index_cache_data
    check(index_cache_data, 'index_cache_data', 'table')

    local lookup_index_name = index_cache_data.lookup_index_name
    local parts_tree = index_cache_data.parts_tree
    local connection_indexes = index_cache_data.connection_indexes

    check(lookup_index_name, 'lookup_index_name', 'table')
    check(parts_tree, 'parts_tree', 'table')
    check(connection_indexes, 'connection_indexes', 'table')

    -- That is the 'slow offset' case. Here we fetch objects by a connection.
    -- If an offset is set we return it as `pivot.filter`. So, select will be
    -- performed by an index from the connection, then the result will be
    -- postprocessed using `pivot`.
    if from.collection_name ~= nil then
        local connection_index =
            connection_indexes[collection_name][from.connection_name]
        local index_name = connection_index.index_name
        assert(index_name ~= nil, 'index_name must not be nil')
        -- offset in this case uses frontend filtering, so we should not set
        -- full_match if it is passed
        local full_match = next(filter) == nil and
            not are_list_args_can_reduce_result(args) and args.offset == nil
        local value_list = from.destination_args_values

        local pivot
        if args.offset ~= nil then
            local _, index_meta = db_schema_helpers.get_primary_index_meta(
                self.db_schema, collection_name)
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

        return full_match, index_name, filter, value_list, pivot
    end

    -- The 'fast offset' case. Here we fetch top-level objects starting from
    -- passed offset. Select will be performed by the primary index and
    -- corresponding offset in `pivot.value_list`, then the result will be
    -- postprocessed using `filter`, if necessary.
    if args.offset ~= nil then
        local index_name, index_meta = db_schema_helpers.get_primary_index_meta(
            self.db_schema, collection_name)
        local pivot_value_list
        if type(args.offset) == 'table' then
            local offset_full_match
            offset_full_match, pivot_value_list = flatten_filter(
                self.db_schema, args.offset, collection_name, index_name)
            assert(offset_full_match == true,
                'offset by a partial key is forbidden')
        else
            assert(#index_meta.fields == 1,
                ('index parts count is not 1 for scalar offset: ' ..
                'index "%s"'):format(index_name))
            pivot_value_list = {args.offset}
        end
        local pivot = {value_list = pivot_value_list}
        local full_match = next(filter) == nil and
            not are_list_args_can_reduce_result(args)
        return full_match, index_name, filter, nil, pivot
    end

    -- The 'no offset' case. Here we fetch top-level object either by found
    -- index or using full scan (if the index was not found).

    -- try to find full index
    local name_list_str = filter_names_fingerprint(filter)
    assert(lookup_index_name[collection_name] ~= nil,
        ('cannot find any index for collection "%s"'):format(collection_name))
    local index_name = lookup_index_name[collection_name][name_list_str]
    local value_list = nil
    local new_filter = filter

    -- try to find partial index
    if index_name == nil then
        local root = parts_tree[collection_name]
        index_name = get_best_matched_index(root, filter)
    end

    -- fill value_list and new_filter appropriatelly
    if index_name ~= nil then
        -- it does not matter for 'full_match' whether we use full or partial
        -- index key
        local _
        _, value_list, new_filter = flatten_filter(self.db_schema, filter,
            collection_name, index_name)
    end

    local full_match = next(new_filter) == nil and
        not are_list_args_can_reduce_result(args)

    return full_match, index_name, new_filter, value_list
end

-- }}}

-- Public API {{{

--- Create new index finder instance.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @treturn table new instance
function find_index.new(db_schema)
    local indexes = db_schema.indexes
    local collections = db_schema.collections

    check(indexes, 'indexes', 'table')
    check(collections, 'collections', 'table')

    local index_cache_data = build_index_cache(indexes, collections)

    return setmetatable({
        db_schema = db_schema,
        index_cache_data = index_cache_data,
    }, {
        __index = {
            find = get_index_name,
        }
    })
end

-- }}}

return find_index

--- Data accessor module that is base for `accessor_space` and
--- `accessor_shard` ones.
---
--- It provides basic logic for such space-like data storages and abstracted
--- away from details from where tuples are arrived into the application.

local json = require('json')
local avro_schema = require('avro_schema')
local utils = require('graphql.utils')

local accessor_general = {}
local DEF_RESULTING_OBJECT_CNT_MAX = 10000
local DEF_FETCHED_OBJECT_CNT_MAX = 10000

--- Validate and compile set of avro schemas (with respect to service fields).
--- @tparam table schemas map where keys are string names and values are
--- avro schemas; consider an example in @{tarantool_graphql.new} function
--- description.
--- @tparam table service_fields map where keys are string names of avro
--- schemas (from `schemas` argument) and values are service fields descriptions;
--- consider the example in the @{new} function description.
local function compile_schemas(schemas, service_fields)
    local service_fields_types = {}
    for name, service_fields_list in pairs(service_fields) do
        assert(type(name) == 'string',
            'service_fields key must be a string, got ' .. type(name))
        assert(type(service_fields_list) == 'table',
            'service_fields_list must be a table, got ' ..
            type(service_fields_list))
        local sf_types = {}
        for _, v in ipairs(service_fields_list) do
            assert(type(v) == 'table',
                'service_fields_list item must be a table, got ' .. type(v))
            assert(type(v.name) == 'string',
                'service_field name must be a string, got ' .. type(v.name))
            assert(type(v.type) == 'string',
                'service_field type must be a string, got ' .. type(v.type))
            assert(v.default ~= nil, 'service_field default must not be a nil')
            sf_types[#sf_types + 1] = v.type
        end
        service_fields_types[name] = sf_types
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
    return models
end

--- Get user-provided meta-information about the primary index of given
--- collection.
---
--- @tparam table self the data accessor
---
--- @tparam string collection_name the name of collection to find the primary
--- index
---
--- @treturn string `index_name`
--- @treturn table `index` (meta-information, not the index itself)
local function get_primary_index_meta(self, collection_name)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' ..
        type(collection_name))

    local indexes = self.indexes[collection_name]
    for index_name, index in pairs(indexes) do
        if index.primary then
            return index_name, index
        end
    end

    error(('cannot find primary index for collection "%s"'):format(
        collection_name))
end

--- Get a key to lookup index by `lookup_index_name` (part of `index_cache`).
---
--- @tparam table filter filter for objects, its keys (names of fields) will
--- form the result
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

-- XXX: raw idea: we can store field-to-field_no mapping when creating
-- `lookup_index_name` to faster form the value_list

--- Flatten filter values (transform to a list) against specific index to
--- passing it to index:pairs().
---
--- Only full keys are supported for a compound index for now.
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
local function flatten_filter(self, filter, collection_name, index_name)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))
    assert(type(index_name) == 'string',
        'index_name must be a string, got ' .. type(index_name))

    local value_list = {}

    -- fill value_list
    local index_meta = self.indexes[collection_name][index_name]
    -- XXX: support or remove indexes by service_fields
    assert(#index_meta.service_fields == 0,
        'service_fields support does not implemented yet')
    for _, field_name in ipairs(index_meta.fields) do
        local value = filter[field_name]
        if value == nil then break end
        value_list[#value_list + 1] = value
    end

    -- check for correctness: non-empty value_list
    if #value_list == 0 then -- avoid extra json.encode()
        assert(#value_list > 0,
            ('empty index key: filter: %s, index_name: %s'):format(
            json.encode(filter), index_name))
    end

    -- check for correctness: all filter fields are used
    local count = 0
    for k, v in pairs(filter) do
        count = count + 1
    end
    if count ~= #value_list then -- avoid extra json.encode()
        assert(count ~= #value_list,
            ('filter items count does not match index fields count: ' ..
            'filter: %s, index_name: %s'):format(json.encode(filter),
            index_name))
    end

    local full_match = #value_list == #index_meta.fields
    return full_match, value_list
end

-- XXX: support partial match for primary/secondary indexes and support to skip
-- fields to get an index (full_match must be false in the case because
-- returned items will be additionally filtered after unflatten).

--- Choose an index for lookup tuple(s) by a 'filter'. The filter holds fields
--- values of object(s) we want to find. It uses prebuilt `lookup_index_name`
--- table representing available indexes, which created by the
--- `build_lookup_index_name` function.
---
--- @tparam table self the data accessor created by the `new` function
--- (directly or indirectly using the `accessor_space.new` or the
--- `accessor_shard.new` function); this function uses the
--- `self.index_cache` prebuild table representing available indexes

--- @tparam string collection_name name of a collection of whose indexes the
--- function will search through
---
--- @tparam table from information about a connection bring executor to select
--- from a current collection; it is nil when the executor selecting top-level
--- objects, but has the following structure for nested collections:
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
--- it is the `list_args_instance` variable in terms of the `tarantool_graphql`
--- module; here we using only `args.offset` value
---
--- @treturn boolean `full_match` is whether passing `value_list` to the index
--- with name `index_name` will give tuple(s) proven to match the filter or
--- just some subset of all tuples in the collection which need to be filtered
--- further
---
--- @treturn string `index_name` is name of the found index or nil
---
--- @treturn table `value_list` is values list from the `filter` argument
--- ordered in the such way that can be passed to the found index (has some
--- meaning only when `index_name ~= nil`)
---
--- @treturn table `pivot` (optional) an offset argument represented depending
--- of a case: whether we'll lookup for the offset by an index; it is either
--- `nil`, or contains `value_list` field to pass to a GT (great-then) index,
--- or contains `filter` field to use in `process_tuple` for find the pivot in
--- a select result
local get_index_name = function(self, collection_name, from, filter, args)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' .. type(collection_name))
    assert(from == nil or type(from) == 'table',
        'from must be nil or a table, got ' .. type(from))
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

    local connection_indexes = index_cache.connection_indexes
    assert(type(connection_indexes) == 'table',
        'connection_indexes must be a table, got ' .. type(connection_indexes))

    -- That is the 'slow offset' case. Here we fetch objects by a connection.
    -- If an offset is set we return it as `pivot.filter`. So, select will be
    -- performed by an index from the connection, then the result will be
    -- postprocessed using `pivot`.
    if from ~= nil then
        local connection_index =
            connection_indexes[collection_name][from.connection_name]
        local index_name = connection_index.index_name
        local connection_type = connection_index.connection_type
        assert(index_name ~= nil, 'index_name must not be nil')
        assert(connection_type ~= nil, 'connection_type must not be nil')
        local full_match = connection_type == '1:1' and next(filter) == nil
        local value_list = from.destination_args_values

        local pivot
        if args.offset ~= nil then
            local _, index_meta = get_primary_index_meta(self, collection_name)
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

        return full_match, index_name, value_list, pivot
    end

    -- The 'fast offset' case. Here we fetch top-level objects starting from
    -- passed offset. Select will be performed by the primary index and
    -- corresponding offset in `pivot.value_list`, then the result will be
    -- postprocessed using `filter`, if necessary.
    if args.offset ~= nil then
        local index_name, index_meta = get_primary_index_meta(self,
            collection_name)
        local full_match
        local pivot_value_list
        if type(args.offset) == 'table' then
            full_match, pivot_value_list = flatten_filter(self, args.offset,
                collection_name, index_name)
            assert(full_match == true, 'offset by a partial key is forbidden')
        else
            assert(#index_meta.fields == 1,
                ('index parts count is not 1 for scalar offset: ' ..
                'index "%s"'):format(index_name))
            full_match, pivot_value_list = true, {args.offset}
        end
        local pivot = {value_list = pivot_value_list}
        full_match = full_match and next(filter) == nil
        return full_match, index_name, filter, pivot
    end

    -- The 'no offset' case. Here we fetch top-level object either by found
    -- index or using full scan (if the index was not found).
    local name_list_str = filter_names_fingerprint(filter)
    assert(lookup_index_name[collection_name] ~= nil,
        ('cannot find any index for collection "%s"'):format(collection_name))
    local index_name = lookup_index_name[collection_name][name_list_str]
    local full_match = false
    local value_list = nil
    if index_name ~= nil then
        full_match, value_list = flatten_filter(self, filter, collection_name,
            index_name)
    end
    return full_match, index_name, value_list
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

--@todo add commentary and fix style
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
        ('connection "%s" of collection "%s" ' ..
        'has destination parts that is not prefix of the index ' ..
        '"%s" parts (destination collection - "%s")'):format(c_name, collection_name,
        c.index_name, c.destination_collection))
        i = i + 1
    end
    local parts_cnt = i - 1

    -- partial index of an unique index is not guaranteed to being
    -- unique
    assert(c_type == '1:N' or parts_cnt == #index_fields,
    ('1:1 connection "%s" of collection "%s" ' ..
    'has less fields than the index "%s" has (destination collection - "%s")' ..
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
--- @{tarantool_graphql.new} function; this is for validate collection against
--- certain set of schemas (no 'dangling' schema names in collections)
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
            -- mind two connections types: simple and union
            if connection.destination_collection then
                assert(type(connection.destination_collection) == 'string',
                'connection.destination_collection must be a string, got ' ..
                type(connection.destination_collection))
                assert(type(connection.parts) == 'table',
                'connection.parts must be a table, got ' ..
                type(connection.parts))
                assert(type(connection.index_name) == 'string',
                'connection.index_name must be a string, got ' ..
                type(connection.index_name))
                return
            elseif connection.variants then
                for _, v in pairs(connection.variants) do
                    assert(v.determinant, 'each variant should have a determinant')
                    assert(type(v.determinant) == 'table', 'variant\'s determinant' ..
                    'end must be a table, got ' .. type(v.determinant))
                    assert(type(v.destination_collection) == 'string',
                    'variant.destination_collection must be a string, got ' ..
                    type(v.destination_collection))
                    assert(type(v.parts) == 'table',
                    'variant.parts must be a table, got ' .. type(v.parts))
                    assert(type(v.index_name) == 'string',
                    'variant.index_name must be a string, got ' ..
                    type(v.index_name))
                end
                return
            else
                assert(false, ('collection doesn\'t have neither destination' ..
                'collection nor variants fields'))
            end
        end
    end
end

--- Perform unflatten, skipping, filtering, limiting of objects. This is the
--- core of the `select_internal` function.
---
--- @tparam table state table variable where the function holds state accross
--- invokes; fields:
---
--- * `count` (number),
--- * `objs` (table, list of objects),
--- * `pivot_found` (boolean),
--- * `statistics` (table, per-query statistics).
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
--- * `resulting_object_cnt_max` (number),
--- * `fetched_object_cnt_max` (number).
---
--- @return nil
---
--- Nothing returned, but after necessary count of invokes `state.objs` will
--- hold list of resulting objects.
local function process_tuple(state, tuple, opts)
    local limit = opts.limit
    local filter = opts.filter
    local do_filter = opts.do_filter
    local pivot_filter = opts.pivot_filter
    local qstats = state.statistics
    local resulting_object_cnt_max = opts.resulting_object_cnt_max
    local fetched_object_cnt_max = opts.fetched_object_cnt_max
    qstats.fetched_object_cnt = qstats.fetched_object_cnt + 1
    assert(qstats.fetched_object_cnt <= fetched_object_cnt_max,
            ('fetched object count[%d] exceeds limit[%d] ' ..
                    '(`fetched_object_cnt_max` in accessor)'):format(
                    qstats.fetched_object_cnt, fetched_object_cnt_max))

    -- convert tuple -> object
    local obj = opts.unflatten_tuple(opts.collection_name, tuple,
        opts.default_unflatten_tuple)

    -- skip all items before pivot (the item pointed by offset)
    if not state.pivot_found and pivot_filter then
        local match = utils.is_subtable(obj, pivot_filter)
        if not match then return true end
        state.pivot_found = true
        return true -- skip pivot item too
    end

    -- filter out non-matching objects
    local match = utils.is_subtable(obj, filter)
    if do_filter then
        if not match then return true end
    else
        assert(match, 'found object do not fit passed filter: ' ..
            json.encode(obj))
    end

    -- add the matching object, update count and check limit
    state.objs[#state.objs + 1] = obj
    state.count = state.count + 1
    qstats.resulting_object_cnt = qstats.resulting_object_cnt + 1
    assert(qstats.resulting_object_cnt <= resulting_object_cnt_max,
            ('returning object count[%d] exceeds limit[%d] ' ..
                    '(`resulting_object_cnt_max` in accessor)'):format(
                    qstats.resulting_object_cnt, resulting_object_cnt_max))
    if limit ~= nil and state.count >= limit then
        return false
    end
    return true
end

--- The function is core of this module and implements logic of fetching and
--- filtering requested objects.
---
--- @tparam table self the data accessor created by the `new` function
--- (directly or indirectly using the `accessor_space.new` or the
--- `accessor_shard.new` function)
---
--- @tparam string collection_name name of collection to perform select
---
--- @tparam table from collection and connection names we arrive from/by or nil
--- as defined in the `tarantool_graphql.new` function description
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
--- @treturn table list of matching objects
local function select_internal(self, collection_name, from, filter, args, extra)
    assert(type(self) == 'table',
        'self must be a table, got ' .. type(self))
    assert(type(collection_name) == 'string',
        'collection_name must be a string, got ' ..
        type(collection_name))
    assert(from == nil or type(from) == 'table',
        'from must be nil or a string, got ' .. type(from))
    assert(from == nil or type(from.collection_name) == 'string',
        'from must be nil or from.collection_name must be a string, got ' ..
        type((from or {}).collection_name))
    assert(from == nil or type(from.connection_name) == 'string',
        'from must be nil or from.connection_name must be a string, got ' ..
        type((from or {}).connection_name))
    assert(from == nil or type(from.destination_args_names) == 'table',
        'from must be nil or from.destination_args_names must be a table, ' ..
        'got ' .. type((from or {}).destination_args_names))
    assert(from == nil or type(from.destination_args_values) == 'table',
        'from must be nil or from.destination_args_values must be a table, ' ..
        'got ' .. type((from or {}).destination_args_values))
    assert(type(filter) == 'table',
        'filter must be a table, got ' .. type(filter))
    assert(type(args) == 'table',
        'args must be a table, got ' .. type(args))
    assert(args.limit == nil or type(args.limit) == 'number',
        'args.limit must be a number of nil, got ' .. type(args.limit))
    -- XXX: save type at parsing and check here
    --assert(args.offset == nil or type(args.offset) == 'number',
    --    'args.offset must be a number of nil, got ' .. type(args.offset))

    local collection = self.collections[collection_name]
    assert(collection ~= nil,
        ('cannot find the collection "%s"'):format(
        collection_name))

    -- search for suitable index
    local full_match, index_name, index_value, pivot = get_index_name(
        self, collection_name, from, filter, args)
    assert(self.funcs.is_collection_exists(collection_name),
        ('cannot find collection "%s"'):format(collection_name))
    local index = self.funcs.get_index(collection_name, index_name)
    if from ~= nil then
        -- allow fullscan only for a top-level object
        assert(index ~= nil,
            ('cannot find index "%s" in space "%s"'):format(
            index_name, collection_name))
    end

    -- lookup functions for unflattening
    local schema_name = collection.schema_name
    assert(type(schema_name) == 'string',
        'schema_name must be a string, got ' .. type(schema_name))
    local default_unflatten_tuple = self.default_unflatten_tuple[schema_name]
    assert(default_unflatten_tuple ~= nil,
        ('cannot find default_unflatten_tuple for collection "%s"'):format(
        collection_name))

    -- read-write variables for process_tuple
    local select_state = {
        count = 0,
        objs = {},
        pivot_found = false,
        statistics = extra.qcontext.statistics
    }

    -- read only process_tuple options
    local select_opts = {
        limit = args.limit,
        filter = filter,
        do_filter = not full_match,
        pivot_filter = nil, -- filled later if needed
        resulting_object_cnt_max = self.settings.resulting_object_cnt_max,
        fetched_object_cnt_max = self.settings.fetched_object_cnt_max,
        collection_name = collection_name,
        unflatten_tuple = self.funcs.unflatten_tuple,
        default_unflatten_tuple = default_unflatten_tuple,
    }

    if index == nil then
        -- fullscan
        local primary_index = self.funcs.get_primary_index(collection_name)
        for _, tuple in primary_index:pairs() do
            assert(pivot == nil,
                'offset for top-level objects must use a primary index')
            local continue = process_tuple(select_state, tuple, select_opts)
            if not continue then break end
        end
    else
        -- select by index
        local iterator_opts = {}
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

        for _, tuple in index:pairs(index_value, iterator_opts) do
            local continue = process_tuple(select_state, tuple, select_opts)
            if not continue then break end
        end
    end

    local count = select_state.count
    local objs = select_state.objs

    assert(args.limit == nil or count <= args.limit,
        ('count[%d] exceeds limit[%s] (before return)'):format(
        count, args.limit))
    assert(#objs == count,
        ('count[%d] is not equal to objs count[%d]'):format(count, #objs))

    return objs
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
end

--- Create a new data accessor.
---
--- Provided `funcs` argument determines certain functions for retrieving
--- tuples.
---
--- @tparam table opts `schemas`, `collections`, `service_fields` and `indexes`
--- to give the data accessor all needed meta-information re data; the format is
--- shown below; additional attributes `resulting_object_cnt_max` and
--- `fetched_object_cnt_max` are optional positive numbers which help to control
--- query behaviour in case it requires more resources than expected _(default
--- value is 10,000)_
---
--- @tparam table funcs set of functions (`is_collection_exists`, `get_index`,
--- `get_primary_index`, `unflatten_tuple`) allows this abstract data accessor
--- behaves in the certain way (say, like space data accessor or shard data
--- accessor); consider the `accessor_space` and the `accessor_shard` modules
--- documentation for this functions description
---
--- For examples of `opts.schemas` and `opts.collections` consider the
--- @{tarantool_graphql.new} function description.
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
--- @treturn table data accessor instance, a table with the two methods
--- (`select` and `arguments`) as described in the @{tarantool_graphql.new}
--- function description.
function accessor_general.new(opts, funcs)
    assert(type(opts) == 'table',
        'opts must be a table, got ' .. type(opts))
    assert(type(funcs) == 'table',
        'funcs must be a table, got ' .. type(funcs))

    local schemas = opts.schemas
    local collections = opts.collections
    local service_fields = opts.service_fields
    local indexes = opts.indexes
    local resulting_object_cnt_max = opts.resulting_object_cnt_max or
                                     DEF_RESULTING_OBJECT_CNT_MAX
    local fetched_object_cnt_max = opts.fetched_object_cnt_max or
                                   DEF_FETCHED_OBJECT_CNT_MAX

    assert(type(schemas) == 'table',
        'schemas must be a table, got ' .. type(schemas))
    assert(type(collections) == 'table',
        'collections must be a table, got ' .. type(collections))
    assert(type(service_fields) == 'table',
        'service_fields must be a table, got ' .. type(service_fields))
    assert(type(indexes) == 'table',
        'indexes must be a table, got ' .. type(indexes))
    assert(type(resulting_object_cnt_max) == 'number' and
                resulting_object_cnt_max > 0,
        'resulting_object_cnt_max must be natural number')
    assert(type(fetched_object_cnt_max) == 'number' and
                fetched_object_cnt_max > 0,
        'fetched_object_cnt_max must be natural number')

    local models = compile_schemas(schemas, service_fields)
    validate_collections(collections, schemas, indexes)
    local index_cache = build_index_cache(indexes, collections)

    -- create default unflatten functions, that can be called from
    -- funcs.unflatten_tuple when an additional pre/postprocessing is not
    -- needed
    local default_unflatten_tuple = {}
    for schema_name, model in pairs(models) do
        default_unflatten_tuple[schema_name] =
            function(_, tuple)
                local ok, obj = model.unflatten(tuple)
                assert(ok, 'cannot unflat tuple: ' .. tostring(obj))
                return obj
            end
    end

    validate_funcs(funcs)

    return setmetatable({
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
        models = models,
        default_unflatten_tuple = default_unflatten_tuple,
        index_cache = index_cache,
        funcs = funcs,
        settings = {
            resulting_object_cnt_max = resulting_object_cnt_max,
            fetched_object_cnt_max = fetched_object_cnt_max
        }
    }, {
        -- 'filter' param are typical GraphQL args
        -- 'args' param are list args
        __index = {
            select = function(self, parent, collection_name, from,
                    filter, args, extra)
                assert(type(parent) == 'table',
                    'parent must be a table, got ' .. type(parent))
                assert(from == nil or type(from) == 'table',
                    'from must be nil or a string, got ' .. type(from))
                assert(from == nil or type(from.collection_name) == 'string',
                    'from must be nil or from.collection_name ' ..
                    'must be a string, got ' ..
                    type((from or {}).collection_name))
                assert(from == nil or type(from.connection_name) == 'string',
                    'from must be nil or from.connection_name ' ..
                    'must be a string, got ' ..
                    type((from or {}).connection_name))
                -- use `extra.qcontext` to store per-query variables
                extra.qcontext.statistics = extra.qcontext.statistics or {
                    resulting_object_cnt = 0,
                    fetched_object_cnt = 0
                }
                return select_internal(self, collection_name, from, filter,
                    args, extra)
            end,
            list_args = function(self, collection_name)
                -- get name of field of primary key
                local _, index_meta = get_primary_index_meta(
                    self, collection_name)

                local offset_fields = {}

                for _, field_name in ipairs(index_meta.fields) do
                    local field_type
                    local collection = self.collections[collection_name]
                    local schema = self.schemas[collection.schema_name]
                    for _, field in ipairs(schema.fields) do
                        if field.name == field_name then
                            field_type = field.type
                        end
                    end
                    assert(field_type ~= nil,
                        ('cannot find type for primary index field "%s" ' ..
                        'for collection "%s"'):format(field_name,
                        collection_name))
                    assert(type(field_type) == 'string',
                        'field type must be a string, got ' ..
                        type(field_type))
                    offset_fields[#offset_fields + 1] = {
                        name = field_name,
                        type = field_type,
                    }
                end

                local offset_type
                assert(#offset_fields > 0,
                    'offset must contain at least one field')
                if #offset_fields == 1 then
                    -- use a scalar type
                    offset_type = offset_fields[1].type
                else
                    -- construct an input type
                    offset_type = {
                        name = collection_name .. '_offset',
                        type = 'record',
                        fields = offset_fields,
                    }
                end

                return {
                    {name = 'limit', type = 'int'},
                    {name = 'offset', type = offset_type},
                    -- {name = 'filter', type = ...},
                }
            end,
        }
    })
end

return accessor_general

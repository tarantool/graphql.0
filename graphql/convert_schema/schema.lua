--- Convert extended avro-schema (collections) to GraphQL schema.

local log = require('log')
local core_types = require('graphql.core.types')
local core_schema = require('graphql.core.schema')
local gen_arguments = require('graphql.gen_arguments')
local arguments = require('graphql.convert_schema.arguments')
local types = require('graphql.convert_schema.types')
local avro_helpers = require('graphql.avro_helpers')

local utils = require('graphql.utils')
local check = utils.check

local schema = {}

--- Add extra arguments for collection / connection fields.
---
--- XXX: This function is written in the hacky way. The function should gone
--- when we'll rewrite argument / InputObject generation in the right way. The
--- plan is the following:
---
--- * DONE: Move object_args to accessor_general (or move all *_args function
---   into a separate module); skipping float / double / ... arguments should
---   be done here.
--- * TBD: generate per-connection arguments in avro-schema in some way?
--- * DONE: Move avro-schema -> GraphQL arguments translating into its own
---   module.
--- * DONE: Support a sub-record arguments and others (union, array, ...).
--- * TBD: Generate arguments for cartesian product of {1:1, 1:N, all} x
---   {query, mutation, all} x {top-level, nested, all} x {collections}.
--- * TBD: Use generated arguments in GraphQL types (schema) generation.
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table root_types generated by @{create_root_collection}
---
--- @return nothing
local function add_extra_arguments(state, root_types)
    for _, what in ipairs({'Query', 'Mutation'}) do
        -- add extra arguments to top-level fields (collections)
        for collection_name, field in pairs(root_types[what].fields) do
            -- Prevent exposing an argument inserted, say, into the mutation schema
            -- subtree to the query subtree (it is needed because we use a booking
            -- table for arguments).
            field.arguments = table.copy(field.arguments)

            local extra_args = state.extra_arguments[collection_name]
            local extra_args_meta = state.extra_arguments_meta[collection_name]

            for arg_name, arg in pairs(extra_args) do
                local meta = extra_args_meta[arg_name]
                check(meta, 'meta', 'table')
                local add_arg = what == 'Mutation' or
                    not meta.add_to_mutations_only
                if add_arg then
                    field.arguments[arg_name] = arg
                end
            end

            local parent_field = field

            local collection = state.collections[collection_name]
            for _, c in ipairs(collection.connections or {}) do
                -- XXX: support multihead connections
                if c.destination_collection then
                    local collection_name = c.destination_collection
                    local field = core_types.bare(
                        parent_field.kind).fields[c.name]
                    local extra_args = state.extra_arguments[collection_name]
                    local extra_args_meta =
                        state.extra_arguments_meta[collection_name]

                    for arg_name, arg in pairs(extra_args) do
                        local meta = extra_args_meta[arg_name]
                        check(meta, 'meta', 'table')
                        local add_arg = not meta.add_to_top_fields_only and
                            (what == 'Mutation' or
                            not meta.add_to_mutations_only)
                        if add_arg then
                            field.arguments[arg_name] = arg
                        end
                    end
                end
            end
        end
    end
end

--- Create virtual root collections `query` and `mutation`, which has
--- connections to any collection.
---
--- Actually, each GQL query starts its execution from the `query` or
--- `mutation` collection. That is why it shoult contain connections to any
--- collection.
---
--- @tparam table state dictionary which contains all information about the
--- schema, arguments, types...
local function create_root_collection(state)
    local root_connections = {}
    -- The fake connections have 1:N mechanics.
    -- Create one connection for each collection.
    for collection_name, collection in pairs(state.collections) do
        table.insert(root_connections, {
            parts = {},
            name = collection_name,
            destination_collection = collection_name,
            type = "1:N"
        })
    end

    local root_types = {}

    for _, what in ipairs({'Query', 'Mutation'}) do
        local root_schema = {
            type = "record",
            name = what,
            -- The fake root has no fields.
            fields = {}
        }
        local root_collection = {
            name = nil, -- skip are_all_parts_null check, see resolve.lua
            connections = root_connections
        }

        -- `convert` is designed to create GQL type corresponding to a real
        -- schema and connections. However it also works with the fake schema.
        -- Query/Mutation type must be the Object, so it cannot be nonNull.
        root_types[what] = core_types.nullable(
            types.convert(state, root_schema, {
                collection = root_collection,
            }))
    end

    add_extra_arguments(state, root_types)

    state.schema = core_schema.create({
        query = root_types['Query'],
        mutation = root_types['Mutation'],
    })
end

--- Execute a function for each connection of one of specified types in each
--- collection.
---
--- @tparam table state tarantool_graphql instance
---
--- @tparam table[opt] connection_types list of connection types to call `func`
--- on it; nil/box.NULL means all connections w/o filtering
---
--- @tparam function func a function with the following parameters:
---
--- * source collection name (string);
--- * connection (table).
local function for_each_connection(state, connection_types, func)
    for collection_name, collection in pairs(state.collections) do
        for _, c in ipairs(collection.connections or {}) do
            if connection_types == nil or utils.value_in(c.type,
                    connection_types) then
                func(collection_name, c)
            end
        end
    end
end

--- Add arguments corresponding to connections (nested filters).
---
--- @tparam table state graphql_tarantool instance
local function add_connection_arguments(state)
    -- map destination collection to list of input objects
    local input_objects = {}
    -- map source collection and connection name to an input object
    local lookup_input_objects = {}

    -- create InputObjects for each connection of each collection
    for_each_connection(state, {'1:1', '1:N'}, function(collection_name, c)
        -- XXX: support multihead connections
        if c.variants ~= nil then return end

        local object = core_types.inputObject({
            name = c.name,
            description = ('generated from the connection "%s" ' ..
                'of collection "%s" using collection "%s"'):format(
                c.name, collection_name, c.destination_collection),
            fields = state.object_arguments[c.destination_collection],
        })

        if input_objects[c.destination_collection] == nil then
            input_objects[c.destination_collection] = {}
        end
        table.insert(input_objects[c.destination_collection], object)

        if lookup_input_objects[collection_name] == nil then
            lookup_input_objects[collection_name] = {}
        end
        lookup_input_objects[collection_name][c.name] = object
    end)

    -- update fields of collection arguments and input objects with other input
    -- objects
    for_each_connection(state, {'1:1', '1:N'}, function(collection_name, c)
        -- XXX: support multihead connections
        if c.variants ~= nil then return end

        local new_object = lookup_input_objects[collection_name][c.name]
        -- collection arguments
        local fields = state.object_arguments[collection_name]
        assert(fields[c.name] == nil,
            'we must not add an input object twice to the same collection ' ..
            'arguments list')
        fields[c.name] = new_object
        -- input objects
        for _, input_object in ipairs(input_objects[collection_name] or {}) do
            local fields = input_object.fields
            assert(fields[c.name] == nil,
                'we must not add an input object twice to the same input ' ..
                'object')
            fields[c.name] = {
                name = c.name,
                kind = new_object,
            }
        end
    end)
end

--- Check there are no connections named as uppermost fields of a schema and
--- there are no connections with the same name.
local function check_connection_names(schema, collection)
    assert(collection.name ~= nil)

    local connection_names = {}
    local field_names = {}

    for _, field in ipairs(schema.fields) do
        local name = field.name
        assert(name ~= nil)
        assert(field_names[name] == nil)
        field_names[name] = true
    end

    for _, connection in ipairs(collection.connections) do
        local name = connection.name
        assert(name ~= nil)
        if connection_names[name] ~= nil then
            local err = ('[collection "%s"] two connections are named "%s"')
                :format(collection.name, name)
            error(err)
        end
        if field_names[name] ~= nil then
            local err = ('[collection "%s"] the connection "%s" is named ' ..
                'as a schema field'):format(collection.name, name)
            error(err)
        end
        connection_names[name] = true
        field_names[name] = true
    end
end

function schema.convert(state, cfg)
    -- collection type is always record, so always non-null; we can lazily
    -- evaluate non-null type from nullable type, but not vice versa, so we
    -- collect nullable types here and evaluate non-null ones where needed
    state.nullable_collection_types = utils.gen_booking_table({})

    state.object_arguments = utils.gen_booking_table({})
    state.list_arguments = utils.gen_booking_table({})
    state.all_arguments = utils.gen_booking_table({})

    -- Booking table used here because of the one reason: inside a resolve
    -- function we need to determine that a user-provided argument is an extra
    -- argument. We capture extra_arguments[collection_name] into the resolve
    -- function and sure it exists and will not be changed.
    state.extra_arguments = utils.gen_booking_table({})
    state.extra_arguments_meta = {}

    local accessor = cfg.accessor
    assert(accessor ~= nil, 'cfg.accessor must not be nil')
    assert(accessor.select ~= nil, 'cfg.accessor.select must not be nil')
    state.accessor = accessor

    assert(cfg.collections ~= nil, 'cfg.collections must not be nil')
    local collections = table.copy(cfg.collections)
    state.collections = collections

    -- add schemas with expanded references
    cfg.e_schemas = {}

    -- Prepare types which represents:
    --  - Avro schemas (collections)
    --  - scalar field arguments (used to filter objects by value stored in it's
    --    field)
    --  - list arguments (offset, limit...)
    for collection_name, collection in pairs(state.collections) do
        -- add name field into each collection
        collection.name = collection_name
        check(collection.name, 'collection.name', 'string')

        assert(collection.schema_name ~= nil,
            'collection.schema_name must not be nil')

        local schema = cfg.schemas[collection.schema_name]
        assert(schema ~= nil, ('cfg.schemas[%s] must not be nil'):format(
            tostring(collection.schema_name)))
        assert(schema.name == nil or schema.name == collection.schema_name,
            ('top-level schema name does not match the name in ' ..
            'the schema itself: "%s" vs "%s"'):format(collection.schema_name,
            schema.name))

        assert(schema.type == 'record',
            'top-level schema must have record avro type, got ' ..
            tostring(schema.type))

        check_connection_names(schema, collection)

        -- fill schema with expanded references
        local e_schema = avro_helpers.expand_references(schema)
        cfg.e_schemas[collection.schema_name] = e_schema

        -- recursively converts all avro types into GraphQL types in the given
        -- schema
        local collection_type = types.convert(state, e_schema, {
            collection = collection,
            type_name = collection_name,
        })
        -- we utilize the fact that collection type is always non-null and
        -- don't store this information; see comment above for
        -- `nullable_collection_types` variable definition
        assert(collection_type.__type == 'NonNull',
            'collection must always has non-null type')
        state.nullable_collection_types[collection_name] =
            core_types.nullable(collection_type)

        -- prepare arguments' types
        local object_args_avro = gen_arguments.object_args(cfg, collection_name)
        local list_args_avro = gen_arguments.list_args(cfg, collection_name)
        local extra_args_opts = {
            enable_mutations = accessor.settings.enable_mutations,
        }
        local extra_args_avro, extra_args_meta = gen_arguments.extra_args(cfg,
            collection_name, extra_args_opts)
        check(extra_args_meta, 'extra_args_meta', 'table')

        local object_args = arguments.convert_record_fields(object_args_avro,
            collection_name)
        local list_args = arguments.convert_record_fields(list_args_avro,
            collection_name)
        local extra_args = arguments.convert_record_fields(extra_args_avro,
            collection_name)

        state.object_arguments[collection_name] = object_args
        state.list_arguments[collection_name] = list_args
        state.extra_arguments[collection_name] = extra_args
        state.extra_arguments_meta[collection_name] = extra_args_meta
    end

    add_connection_arguments(state)

    -- fill all_arguments with object_arguments + list_arguments
    for collection_name, collection in pairs(state.collections) do
        local object_args = state.object_arguments[collection_name]
        local list_args = state.list_arguments[collection_name]

        -- check for names clash
        for name, _ in pairs(list_args) do
            if object_args[name] ~= nil then
                local err = ('the argument "%s" generated from the same ' ..
                    'named field of the collection "%s" is superseded with ' ..
                    'the list filtering argument "%s"'):format(name,
                    collection_name, name)
                log.warn(err)
            end
        end

        local args = utils.merge_tables(object_args, list_args)
        state.all_arguments[collection_name] = args
    end

    -- create fake root for the `query` and the `mutation` collection
    create_root_collection(state)
end

return schema

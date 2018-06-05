--- Generate avro-schema for arguments by given database schema.

local utils = require('graphql.utils')
local rex, _ = utils.optional_require_rex()
local avro_helpers = require('graphql.avro_helpers')
local db_schema_helpers = require('graphql.db_schema_helpers')

local gen_arguments = {}

--- Get an avro-schema for a primary key by a collection name.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @tparam string collection_name name of a collection
---
--- @treturn string `offset_type` is a just string in case of scalar primary
--- key (and, then, offset) type
---
--- @treturn table `offset_type` is a record in case of compound (multi-part)
--- primary key
local function get_primary_key_type(db_schema, collection_name)
    -- get name of field of primary key
    local _, index_meta = db_schema_helpers.get_primary_index_meta(
        db_schema, collection_name)

    local collection = db_schema.collections[collection_name]
    local schema = db_schema.schemas[collection.schema_name]

    local offset_fields = {}

    for _, field_name in ipairs(index_meta.fields) do
        local field_type
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

    return offset_type
end

-- XXX: add string fields of a nested record / 1:1 connection to
-- get_pcre_argument_type

--- Get an avro-schema for a pcre argument by a collection name.
---
--- Note: it is called from `list_args`, so applicable only for lists:
--- top-level objects and 1:N connections.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @tparam string collection_name name of a collection
---
--- @treturn table `pcre_type` is a record with fields per string/string* field
--- of an object of the collection
local function get_pcre_argument_type(db_schema, collection_name)
    local collection = db_schema.collections[collection_name]
    assert(collection ~= nil, 'cannot found collection ' ..
        tostring(collection_name))
    local schema = db_schema.schemas[collection.schema_name]
    assert(schema ~= nil, 'cannot found schema ' ..
        tostring(collection.schema_name))

    assert(schema.type == 'record',
        'top-level object expected to be a record, got ' ..
        tostring(schema.type))

    local string_fields = {}

    for _, field in ipairs(schema.fields) do
        if field.type == 'string' or field.type == 'string*' then
            local field = table.copy(field)
            field.type = avro_helpers.make_avro_type_nullable(
                field.type, {raise_on_nullable = false})
            table.insert(string_fields, field)
        end
    end

    local pcre_type = {
        name = collection_name .. '_pcre',
        type = 'record',
        fields = string_fields,
    }
    return pcre_type
end

--- List of avro-schema fields to use as arguments of a collection field and
--- 1:N connection field.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @tparam string collection_name name of collection to create the fields
---
--- @treturn table list of avro-schema fields
function gen_arguments.list_args(db_schema, collection_name)
    local offset_type = get_primary_key_type(db_schema, collection_name)

    -- add `pcre` argument only if lrexlib-pcre was found
    local pcre_field
    if rex ~= nil then
        local pcre_type = get_pcre_argument_type(db_schema, collection_name)
        pcre_field = {name = 'pcre', type = pcre_type}
    end

    return {
        {name = 'limit', type = 'int'},
        {name = 'offset', type = offset_type},
        -- {name = 'filter', type = ...},
        pcre_field,
    }
end

--- List of avro-schema fields to use as extra arguments of a collection /
--- a connection field.
---
--- Mutation arguments (insert, update, delete) are generated here.
---
--- @tparam table db_schema `schemas`, `collections`, `service_fields`,
--- `indexes`
---
--- @tparam string collection_name name of collection to create the fields
---
--- @tparam table opts
---
--- * enable_mutations (boolean)
---
--- @treturn table list of avro-schema fields
---
--- @treturn table map with flags to describe where generated arguments should
--- be used; the format is the following:
---
---    {
---        <field name> = {
---            add_to_mutations_only = <boolean>,
---            add_to_top_fields_only = <boolean>,
---        },
---        ...
---    }
function gen_arguments.extra_args(db_schema, collection_name, opts)
    local opts = opts or {}
    local enable_mutations = opts.enable_mutations or false

    if not enable_mutations then
        return {}, {}
    end

    local collection = db_schema.collections[collection_name]
    local schema_name = collection.schema_name

    local schema_insert = table.copy(db_schema.schemas[schema_name])
    schema_insert.name = collection_name .. '_insert'

    local _, primary_index_meta = db_schema_helpers.get_primary_index_meta(
        db_schema, collection_name)

    local schema_update = {
        name = collection_name .. '_update',
        type = 'record',
        fields = {},
    }
    -- add all fields except ones whose are part of the primary key
    for _, field in ipairs(db_schema.schemas[schema_name].fields) do
        assert(field.name ~= nil, 'field.name is nil')
        local is_field_part_of_primary_key = false
        for _, pk_field_name in ipairs(primary_index_meta.fields) do
            if field.name == pk_field_name then
                is_field_part_of_primary_key = true
                break
            end
        end

        if not is_field_part_of_primary_key then
            local field = table.copy(field)
            field.type = avro_helpers.make_avro_type_nullable(
                field.type)
            table.insert(schema_update.fields, field)
        end
    end

    local schema_delete = 'boolean'

    return {
        {name = 'insert', type = schema_insert},
        {name = 'update', type = schema_update},
        {name = 'delete', type = schema_delete},
    }, {
        insert = {
            add_to_mutations_only = true,
            add_to_top_fields_only = true,
        },
        update = {
            add_to_mutations_only = true,
            add_to_top_fields_only = false,
        },
        delete = {
            add_to_mutations_only = true,
            add_to_top_fields_only = false,
        },
    }
end

return gen_arguments

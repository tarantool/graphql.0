--- Generate avro-schema for arguments by given database schema.

local json = require('json')
local utils = require('graphql.utils')
local rex, _ = utils.optional_require_rex()
local avro_helpers = require('graphql.avro_helpers')
local db_schema_helpers = require('graphql.db_schema_helpers')

local check = utils.check

local gen_arguments = {}

--- Get an avro-schema for a primary key by a collection name.
---
--- @tparam table db_schema `e_schemas`, `schemas`, `collections`,
--- `service_fields`, `indexes`
---
--- @tparam string collection_name name of a collection
---
--- @treturn string `offset_type` is a just string in case of scalar primary
--- key (and, then, offset) type
---
--- @treturn table `offset_type` is a record in case of compound (multi-part)
--- primary key (is not nil)
local function get_primary_key_type(db_schema, collection_name)
    -- get name of field of primary key
    local _, index_meta = db_schema_helpers.get_primary_index_meta(
        db_schema, collection_name)

    local schema_name = db_schema_helpers.get_schema_name(db_schema,
        collection_name)
    local e_schema = db_schema.e_schemas[schema_name]

    local offset_fields = {}

    for _, field_name in ipairs(index_meta.fields) do
        local field_type
        for _, field in ipairs(e_schema.fields) do
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
        if field_type ~= nil then
            offset_fields[#offset_fields + 1] = {
                name = field_name,
                type = field_type,
            }
        end
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

--- Make schema types deep nullable down to scalar, union, array or map
--- (matches xflatten input syntax) and remove default values (from fields of
--- record and record* types).
---
--- @param e_schema (table or string) avro-schema with expanded references
---
--- @tparam[opt] function skip_cond
---
--- @return transformed avro-schema or nil (when the type or all its fields are
--- matched by skip_cond)
local function recursive_nullable(e_schema, skip_cond)
    local avro_t = avro_helpers.avro_type(e_schema)

    if skip_cond ~= nil and skip_cond(avro_t) then return nil end

    if avro_helpers.is_scalar_type(avro_t) then
        return avro_helpers.make_avro_type_nullable(e_schema,
            {raise_on_nullable = false})
    elseif avro_t == 'record' or avro_t == 'record*' then
        local res = table.copy(e_schema)
        res.type = 'record*' -- make the record nullable
        res.fields = {}

        for _, field in ipairs(e_schema.fields) do
            local new_type = recursive_nullable(field.type, skip_cond)
            if new_type ~= nil then
                local field = table.copy(field)
                field.type = new_type
                field.default = nil
                table.insert(res.fields, field)
            end
        end

        if #res.fields == 0 then return nil end
        return res
    elseif avro_t == 'union' or
            avro_t == 'array' or avro_t == 'array*' or
            avro_t == 'map' or avro_t == 'map*' then
        -- it is non-recursive intentionally to match current xflatten semantics
        e_schema = table.copy(e_schema)
        return avro_helpers.make_avro_type_nullable(e_schema,
            {raise_on_nullable = false})
    end

    error('unrecognized avro-schema type: ' .. json.encode(e_schema))
end

--- Remove default values from passed avro-schema (from fields of record and
--- record* types) and make they nullable.
---
--- @param e_schema (table or string) avro-schema with expanded references
---
--- @return transformed avro-schema or nil (empty fields case)
local function recursive_replace_default_with_nullable(e_schema)
    local avro_t = avro_helpers.avro_type(e_schema)

    if avro_helpers.is_scalar_type(avro_t) then
        return e_schema
    elseif avro_t == 'record' or avro_t == 'record*' then
        local res = table.copy(e_schema)
        res.fields = {}

        for _, field in ipairs(e_schema.fields) do
            field = table.copy(field)
            if type(field.default) ~= 'nil' then
                field.default = nil
                field.type = avro_helpers.make_avro_type_nullable(field.type,
                    {raise_on_nullable = false})
            end
            field.type = recursive_replace_default_with_nullable(field.type)
            table.insert(res.fields, field)
        end

        if #res.fields == 0 then return nil end
        return res
    elseif avro_t == 'union' then
        local res = {}

        for _, child in ipairs(e_schema) do
            local new_child_type =
                recursive_replace_default_with_nullable(child)
            table.insert(res, new_child_type)
        end

        if #res == 0 then return nil end
        return res
    elseif avro_t == 'array' or avro_t == 'array*' then
        local res = table.copy(e_schema)
        res.items = recursive_replace_default_with_nullable(e_schema.items)
        if res.items == nil then return nil end
        return res
    elseif avro_t == 'map' or avro_t == 'map*' then
        local res = table.copy(e_schema)
        res.values = recursive_replace_default_with_nullable(e_schema.values)
        if res.values == nil then return nil end
        return res
    end

    error('unrecognized avro-schema type: ' .. json.encode(e_schema))
end

--- Whether we can compare the type for equallity.
---
--- @tparam string avro_schema_type
---
--- @treturn boolean
local function is_comparable_scalar_type(avro_schema_type)
    check(avro_schema_type, 'avro_schema_type', 'string')

    local scalar_types = {
        ['int'] = true,
        ['int*'] = true,
        ['long'] = true,
        ['long*'] = true,
        ['boolean'] = true,
        ['boolean*'] = true,
        ['string'] = true,
        ['string*'] = true,
        ['null'] = true,
    }

    return scalar_types[avro_schema_type] or false
end

-- XXX: add string fields of 1:1 connection to get_pcre_argument_type

--- Get an avro-schema for a pcre argument by a collection name.
---
--- Note: it is called from `list_args`, so applicable only for lists:
--- top-level objects and 1:N connections.
---
--- @tparam table db_schema `e_schemas`, `schemas`, `collections`,
--- `service_fields`, `indexes`
---
--- @tparam string collection_name name of a collection
---
--- @treturn table record with fields per string/string* field of an object
--- of the collection
local function get_pcre_argument_type(db_schema, collection_name)
    local schema_name = db_schema_helpers.get_schema_name(db_schema,
        collection_name)
    local e_schema = db_schema.e_schemas[schema_name]
    assert(e_schema ~= nil, 'cannot find expanded schema ' ..
        tostring(schema_name))

    assert(e_schema.type == 'record',
        'top-level object expected to be a record, got ' ..
        tostring(e_schema.type))

    local res = recursive_nullable(e_schema, function(avro_t)
        -- skip non-comparable scalars (float, double), union, array, map
        local is_non_string_scalar = avro_helpers.is_scalar_type(avro_t) and
            (avro_t ~= 'string' and avro_t ~= 'string*')
        local is_non_record_compound = avro_helpers.is_compound_type(avro_t)
            and (avro_t ~= 'record' and avro_t ~= 'record*')
        return is_non_string_scalar or is_non_record_compound

    end)
    if res == nil then return nil end
    res.name = collection_name .. '_pcre'
    return res
end

--- Get avro-schema for update argument.
---
--- @tparam table db_schema `e_schemas`, `schemas`, `collections`,
--- `service_fields`, `indexes`
---
--- @tparam string collection_name name of a collection
---
--- @treturn table generated avro-schema
local function get_update_argument_type(db_schema, collection_name)
    local schema_name = db_schema_helpers.get_schema_name(db_schema,
        collection_name)
    local e_schema = db_schema.e_schemas[schema_name]
    assert(e_schema ~= nil, 'cannot find expanded schema ' ..
        tostring(schema_name))

    assert(e_schema.type == 'record',
        'top-level object expected to be a record, got ' ..
        tostring(e_schema.type))

    local _, primary_index_meta = db_schema_helpers.get_primary_index_meta(
        db_schema, collection_name)

    local schema_update = {
        name = collection_name .. '_update',
        type = 'record*',
        fields = {},
    }
    -- add all fields except ones whose are part of the primary key
    for _, field in ipairs(e_schema.fields) do
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
            field.type = recursive_nullable(field.type)
            if field.type ~= nil then
                table.insert(schema_update.fields, field)
            end
        end
    end

    if schema_update.fields == nil then return nil end

    return schema_update
end

--- List of avro-schema fields to use as arguments of a collection field and a
--- connection field (with any connection type).
---
--- @tparam table db_schema `e_schemas`, `schemas`, `collections`,
--- `service_fields`, `indexes`
---
--- @tparam string collection_name name of collection to create the fields
---
--- @treturn table list of avro-schema fields
function gen_arguments.object_args(db_schema, collection_name)
    local schema_name = db_schema_helpers.get_schema_name(db_schema,
        collection_name)
    local e_schema = db_schema.e_schemas[schema_name]
    assert(e_schema ~= nil, 'cannot find expanded schema ' ..
        tostring(schema_name))

    assert(e_schema.type == 'record',
        'top-level object expected to be a record, got ' ..
        tostring(e_schema.type))

    local res = recursive_nullable(e_schema, function(avro_t)
        -- skip non-comparable scalars (float, double), union, array, map
        local is_non_comparable_scalar = avro_helpers.is_scalar_type(avro_t) and
            not is_comparable_scalar_type(avro_t)
        local is_non_record_compound = avro_helpers.is_compound_type(avro_t)
            and (avro_t ~= 'record' and avro_t ~= 'record*')
        return is_non_comparable_scalar or is_non_record_compound
    end)

    if res == nil then return {} end

    return res.fields
end

--- List of avro-schema fields to use as arguments of a collection field and
--- 1:N connection field.
---
--- @tparam table db_schema `e_schemas`, `schemas`, `collections`,
--- `service_fields`, `indexes`
---
--- @tparam string collection_name name of collection to create the fields
---
--- @treturn table list of avro-schema fields
function gen_arguments.list_args(db_schema, collection_name)
    local offset_type = get_primary_key_type(db_schema, collection_name)
    offset_type = avro_helpers.make_avro_type_nullable(offset_type,
        {raise_on_nullable = false})

    -- add `pcre` argument only if lrexlib-pcre was found
    local pcre_field
    if rex ~= nil then
        local pcre_type = get_pcre_argument_type(db_schema, collection_name)
        if pcre_type ~= nil then
            pcre_field = {name = 'pcre', type = pcre_type}
        end
    end

    return {
        {name = 'limit', type = 'int*'},
        {name = 'offset', type = offset_type},
        {name = 'filter', type = 'string*'},
        pcre_field,
    }
end

--- List of avro-schema fields to use as extra arguments of a collection /
--- a connection field.
---
--- Mutation arguments (insert, update, delete) are generated here.
---
--- @tparam table db_schema `e_schemas`, `schemas`, `collections`,
--- `service_fields`, `indexes`
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

    local schema_name = db_schema_helpers.get_schema_name(db_schema,
        collection_name)
    local e_schema = db_schema.e_schemas[schema_name]

    local schema_insert = recursive_replace_default_with_nullable(e_schema)
    if schema_insert ~= nil then
        schema_insert.name = collection_name .. '_insert'
        schema_insert.type = 'record*' -- make the record nullable
    end

    local schema_update = get_update_argument_type(db_schema, collection_name)
    local schema_delete = 'boolean*'

    local args = {}
    local args_meta = {}

    if schema_insert ~= nil then
        table.insert(args, {name = 'insert', type = schema_insert})
        args_meta.insert = {
            add_to_mutations_only = true,
            add_to_top_fields_only = true,
        }
    end

    if schema_update ~= nil then
        table.insert(args, {name = 'update', type = schema_update})
        args_meta.update = {
            add_to_mutations_only = true,
            add_to_top_fields_only = false,
        }
    end

    if schema_delete ~= nil then
        table.insert(args, {name = 'delete', type = schema_delete})
        args_meta.delete = {
            add_to_mutations_only = true,
            add_to_top_fields_only = false,
        }
    end

    return args, args_meta
end

return gen_arguments

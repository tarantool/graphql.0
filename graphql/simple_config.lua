--- The simple config module provides an ability to generate config (cfg) for
--- tarantool_graphql using tarantool meta-information.
---
------ Explanation:
---
--- * To make use of it you must specify tarantool tuples' format during space
---  creation passing or after it using space_object:format(). Spaces with no
---  formats (both 'name and 'type' fields must be filled) will be ignored.
---  Resulting schemas lack fields of the following types: 'record', 'array'
---  and 'map'. Resulting collections have no connections. Schemas and
---  collections may be complemented.

local check = require('graphql.utils').check

local simple_config = {}

--- The functions tells if given space is a tarantool system space or not.
--- It relies on tarantool implementation's details. The source of the function is
--- space_is_system() in tarantool/src/box/schema.cc
local function is_system_space(space)
    local BOX_SYSTEM_ID_MIN = 256
    local BOX_SYSTEM_ID_MAX = 511
    local space_id = space[1]
    return (BOX_SYSTEM_ID_MIN < space_id and space_id < BOX_SYSTEM_ID_MAX)
end

--- The functions converts given tarantool tuple's (received from space:format())
--- field type into avro-schema type. Notes on tarantool tuple's field type can
--- be found at https://tarantool.org/en/doc/2.0/book/box/data_model.html#indexed-field-types
---
--- XXX scalar type conversion is not implemented yet. Consider using
--- avro unions to implement it.
local function convert_index_type_to_avro(index_type, is_nullable)
    -- unsigned | string | integer | number | boolean | array | scalar
    check(index_type, 'index_type', 'string')

    if index_type == 'scalar' then
        error('scalar type conversion (tarantool types -> avro) is not ' ..
            'implemented yet')
    end

    local index_type_to_avro_type =
        {unsigned = 'long', string = 'string', integer = 'long',
         number = 'double', boolean = 'boolean',
         array = {type = 'array', items = 'long'}}

    local result = index_type_to_avro_type[index_type]

    assert(result, 'index type to avro type conversion failed, as there ' ..
        'were no match for type ' .. index_type)

    if is_nullable then
        return result .. '*'
    else
        return result
    end
end

--- The function generates avro schema using given space format. Note that
--- space format is a flat format so no nested schemas (e.g record inside record)
--- can be generated.
local function generate_avro_schema(space_format, schema_name)
    check(space_format, 'space_format', 'table')
    check(schema_name, 'schema_name', 'string')

    local avro_schema = {type = 'record', name = schema_name, fields = {}}
    for i, f in ipairs(space_format) do
        check(f.name, 'field format name', 'string')
        check(f.type, 'field format type', 'string')
        if not (i == 0 or f.type == 'any') then
            avro_schema.fields[#avro_schema.fields + 1] =
            {name = f.name, type = convert_index_type_to_avro(f.type, f.is_nullable)}
        end
    end
    return avro_schema
end

--- XXX currently only TREE and HASH tarantool index types are supported
local function convert_index_type(index_type)
    assert(type(index_type) == 'string', 'index type must be string, got ' ..
        type(index_type))
    local index_types = {TREE = 'tree', HASH = 'hash'}
    local result = index_types[index_type]
    assert(result, 'index type conversion (from tarantool to graphQL) ' ..
        'failed, as there were no match for type ' .. index_type)
    return result
end

local function extract_collection_indexes(space_name, space_format)
    local collection_indexes = {}
    local i = 0
    local index = box.space[space_name].index[i]
    while index ~= nil do
        local collection_index = {}
        collection_index.index_type = convert_index_type(index.type)
        collection_index.unique = index.unique

        collection_index.primary = (i == 0)

        collection_index.service_fields = {}
        collection_index.fields = {}

        for i, part in ipairs(index.parts) do
            collection_index.fields[i] =
                space_format[part.fieldno].name
        end

        collection_indexes[index.name] = collection_index

        i = i + 1
        index = box.space[space_name].index[i]
    end
    return collection_indexes
end

local function generate_collection(space_name)
    return { schema_name = space_name, connections = {} }
end

--- Tarantool space's format may be defined in different ways:
--- {{'x', 'unsigned', true}, ...}
--- {{name = 'x', type = 'unsigned', is_nullable = true}, ...}
--- {{'x', type = 'unsigned'}, ...}
--- All these ways have the same meaning. The function converts all these
--- formats into the single one:
--- {{name = 'x', type = 'unsigned', is_nullable = true}, ...}
local function unify_format(space_format)
    local resulting_format = {}
    for i, field_format in ipairs(space_format) do
        resulting_format[i] = {}
        resulting_format[i].name = field_format[1] or field_format.name
        resulting_format[i].type = field_format[2] or field_format.type
        resulting_format[i].is_nullable = field_format[3] or field_format.is_nullable
        resulting_format[i].is_nullable = resulting_format[i].is_nullable or false
    end
    return resulting_format
end

local function is_fully_defined(space_format)
    for _, f in ipairs(space_format) do
        if f.name == nil or f.type == nil or f.is_nullable == nil then
            return false
        end
    end
    return true
end

--- The function returns formats of all fully defined spaces.
--- Spaces are taken from the tarantool instance in which
--- tarantool graphql is launched. For definition of fully
--- defined spaces see `is_fully_defined`.
---
--- @treturn table spaces_formats {[space_name] = {space_format}, ...}
--- where space_format is {{first_field_format}, {second_field_format}, ...}
--- and field_format is {[name] = name_string, [type] = type_string,
--- [is_nullable] = boolean_flag}'
function simple_config.get_spaces_formats()
    local spaces_formats = {}
    local FORMAT = 7
    local NAME = 3
    for _, s in box.space._space:pairs() do
        if not is_system_space(s) then
            local space_format = unify_format(s[FORMAT])
            if is_fully_defined(space_format) then
                spaces_formats[s[NAME]] = space_format
            end
        end
    end
    return spaces_formats
end

local function remove_empty_formats(spaces_formats)
    local resulting_formats = table.deepcopy(spaces_formats)
    for space_name, space_format in pairs(resulting_formats) do
        if next(space_format) == nil then
           resulting_formats[space_name] = nil
        end
    end

    return resulting_formats
end

--- The function creates a tarantool graphql config using tarantool metainfo
--- from space:format() and space.index:format(). Notice that this function
--- does not set accessor.
--- @treturn table cfg with `schemas`, `collections`, `has_space_format`,
--- `service_fields` (empty table), `indexes`
function simple_config.graphql_cfg_from_tarantool()
    local cfg = {}
    cfg.schemas = {}
    cfg.service_fields = {}
    cfg.indexes = {}
    cfg.collections = {}
    cfg.collection_use_tomap = {}

    local spaces_formats = simple_config.get_spaces_formats()
    spaces_formats = remove_empty_formats(spaces_formats)
    assert(next(spaces_formats) ~= nil,
        'there are no any spaces with format - can not auto-generate config')

    for space_name, space_format in pairs(spaces_formats) do
        cfg.schemas[space_name] = generate_avro_schema(space_format, space_name)
        cfg.indexes[space_name] =
            extract_collection_indexes(space_name, space_format)
        cfg.service_fields[space_name] = {}
        cfg.collections[space_name] = generate_collection(space_name)
        cfg.collection_use_tomap[space_name] = true
    end
    return cfg
end

return simple_config

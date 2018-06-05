--- Generate resolve functions to connect graphql-lua to accessor_general.

local json = require('json')
local yaml = require('yaml')
local core_types_helpers = require('graphql.convert_schema.core_types_helpers')

local utils = require('graphql.utils')
local check = utils.check

local resolve = {}

local function gen_from_parameter(collection_name, parent, connection)
    local names = {}
    local values = {}

    for _, part in ipairs(connection.parts) do
        check(part.source_field, 'part.source_field', 'string')
        check(part.destination_field, 'part.destination_field', 'string')

        names[#names + 1] = part.destination_field
        values[#values + 1] = parent[part.source_field]
    end

    return {
        collection_name = collection_name,
        connection_name = connection.name,
        destination_args_names = names,
        destination_args_values = values,
    }
end

-- Check FULL match constraint before request of
-- destination object(s). Note that connection key parts
-- can be prefix of index key parts. Zero parts count
-- considered as ok by this check.
local function are_all_parts_null(parent, connection_parts)
    local are_all_parts_null = true
    local are_all_parts_non_null = true
    for _, part in ipairs(connection_parts) do
        local value = parent[part.source_field]

        if value ~= nil then -- nil or box.NULL
            are_all_parts_null = false
        else
            are_all_parts_non_null = false
        end
    end

    local ok = are_all_parts_null or are_all_parts_non_null
    if not ok then -- avoid extra json.encode()
        assert(ok,
            'FULL MATCH constraint was failed: connection ' ..
            'key parts must be all non-nulls or all nulls; ' ..
            'object: ' .. json.encode(parent))
    end

    return are_all_parts_null
end

local function separate_args_instance(args_instance, arguments)
    local object_args_instance = {}
    local list_args_instance = {}
    local extra_args_instance = {}

    for k, v in pairs(args_instance) do
        if arguments.extra[k] ~= nil then
            extra_args_instance[k] = v
        elseif arguments.list[k] ~= nil then
            list_args_instance[k] = v
        elseif arguments.all[k] ~= nil then
            object_args_instance[k] = v
        else
            error(('cannot found "%s" field ("%s" value) ' ..
                'within allowed fields'):format(tostring(k),
                    json.encode(v)))
        end
    end

    return {
        object = object_args_instance,
        list = list_args_instance,
        extra = extra_args_instance,
    }
end

function resolve.gen_resolve_function(collection_name, connection,
        destination_type, arguments, accessor)
    local c = connection
    local raw_destination_type = core_types_helpers.raw_gql_type(
        destination_type)

    -- capture `raw_destination_type`
    local function genResolveField(info)
        return function(field_name, object, filter, opts)
            assert(raw_destination_type.fields[field_name],
                ('performing a subrequest by the non-existent ' ..
                'field "%s" of the collection "%s"'):format(field_name,
                c.destination_collection))
            return raw_destination_type.fields[field_name].resolve(
                object, filter, info, opts)
        end
    end

    -- captures c.{type, parts, name, destination_collection}, collection_name,
    -- genResolveField, arguments, accessor
    return function(parent, args_instance, info, opts)
        local opts = opts or {}
        assert(type(opts) == 'table',
            'opts must be nil or a table, got ' .. type(opts))
        local dont_force_nullability =
            opts.dont_force_nullability or false
        assert(type(dont_force_nullability) == 'boolean',
            'opts.dont_force_nullability ' ..
            'must be nil or a boolean, got ' ..
            type(dont_force_nullability))

        local from = gen_from_parameter(collection_name, parent, c)

        -- Avoid non-needed index lookup on a destination collection when
        -- all connection parts are null:
        -- * return null for 1:1* connection;
        -- * return {} for 1:N connection (except the case when source
        --   collection is the query or the mutation pseudo-collection).
        if collection_name ~= nil and are_all_parts_null(parent, c.parts) then
            if c.type ~= '1:1*' and c.type ~= '1:N' then
                -- `if` is to avoid extra json.encode
                assert(c.type == '1:1*' or c.type == '1:N',
                    ('only 1:1* or 1:N connections can have ' ..
                    'all key parts null; parent is %s from ' ..
                    'collection "%s"'):format(json.encode(parent),
                        tostring(collection_name)))
            end
            return c.type == '1:N' and {} or nil
        end

        local resolveField = genResolveField(info)
        local extra = {
            qcontext = info.qcontext,
            resolveField = resolveField, -- for subrequests
            extra_args = {},
        }

        -- object_args_instance will be passed to 'filter'
        -- list_args_instance will be passed to 'args'
        -- extra_args_instance will be passed to 'extra.extra_args'
        local arguments_instance = separate_args_instance(args_instance,
            arguments)
        extra.extra_args = arguments_instance.extra

        local objs = accessor:select(parent,
            c.destination_collection, from,
            arguments_instance.object, arguments_instance.list, extra)
        assert(type(objs) == 'table',
            'objs list received from an accessor ' ..
            'must be a table, got ' .. type(objs))
        if c.type == '1:1' or c.type == '1:1*' then
            -- we expect here exactly one object even for 1:1*
            -- connections because we processed all-parts-are-null
            -- situation above
            assert(#objs == 1 or dont_force_nullability,
                'expect one matching object, got ' ..
                tostring(#objs))
            return objs[1]
        else -- c.type == '1:N'
            return objs
        end
    end
end

function resolve.gen_resolve_function_multihead(collection_name, connection,
        union_types, var_num_to_box_field_name, accessor)
    local c = connection

    local determinant_keys = utils.get_keys(c.variants[1].determinant)

    local function resolve_variant(parent)
        assert(utils.do_have_keys(parent, determinant_keys),
            ('Parent object of union object doesn\'t have determinant ' ..
            'fields which are necessary to determine which resolving ' ..
            'variant should be used. Union parent object:\n"%s"\n' ..
            'Determinant keys:\n"%s"'):
            format(yaml.encode(parent), yaml.encode(determinant_keys)))

        local var_idx
        local res_var
        for i, var in ipairs(c.variants) do
            local is_match = utils.is_subtable(parent, var.determinant)
            if is_match then
                res_var = var
                var_idx = i
                break
            end
        end

        local box_field_name = var_num_to_box_field_name[var_idx]

        assert(res_var, ('Variant resolving failed.'..
            'Parent object: "%s"\n'):format(yaml.encode(parent)))
        return res_var, var_idx, box_field_name
    end

    return function(parent, _, info)
        local v, variant_num, box_field_name = resolve_variant(parent)
        local destination_type = union_types[variant_num]

        local quazi_connection = {
            type = c.type,
            parts = v.parts,
            name = c.name,
            destination_collection = v.destination_collection,
        }
        -- XXX: generate a function for each variant at schema generation time
        local result = resolve.gen_resolve_function(collection_name,
            quazi_connection, destination_type, {}, accessor)(parent, {}, info)

        -- This 'wrapping' is needed because we use 'select' on 'collection'
        -- GraphQL type and the result of the resolve function must be in
        -- {'collection_name': {result}} format to be avro-valid.
        return {[box_field_name] = result}, destination_type
    end
end

return resolve

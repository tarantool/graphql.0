--- Generate resolve functions to connect graphql-lua to accessor_general.

local json = require('json')
local yaml = require('yaml')
local core_types = require('graphql.core.types')

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

--- Check FULL match constraint before request of destination object(s).
---
--- Note that connection key parts can be prefix of index key parts. Zero parts
--- count considered as ok by this check.
local function are_all_parts_null(parent, connection_parts, opts)
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
    local opts = opts or {}
    local no_assert = opts.no_assert or false
    if not ok and not no_assert then -- avoid extra json.encode()
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

local function invoke_resolve(prepared_resolve)
    if prepared_resolve.is_calculated then
        return prepared_resolve.objs
    end

    local prepared_select = prepared_resolve.prepared_select
    -- local opts = prepared_resolve.opts
    local accessor = prepared_resolve.accessor
    local c = prepared_resolve.connection

    local objs = accessor:invoke_select(prepared_select)
    assert(type(objs) == 'table',
        'objs list received from an accessor ' ..
        'must be a table, got ' .. type(objs))
    if c.type == '1:1' then
        return objs[1] -- nil for empty list of matching objects
    else -- c.type == '1:N'
        return objs
    end
end

function resolve.gen_resolve_function(collection_name, connection,
        destination_type, arguments, accessor, opts)
    local c = connection
    local opts = opts or {}
    local disable_dangling_check = opts.disable_dangling_check or false
    local gen_prepare = opts.gen_prepare or false
    local bare_destination_type = core_types.bare(destination_type)

    -- capture `bare_destination_type`
    local function genResolveField(info)
        return function(field_name, object, filter, opts)
            assert(bare_destination_type.fields[field_name],
                ('performing a subrequest by the non-existent ' ..
                'field "%s" of the collection "%s"'):format(field_name,
                c.destination_collection))
            local opts = table.copy(opts or {})
            opts.is_hidden = true
            local result, field_type =
                bare_destination_type.fields[field_name].resolve(object,
                filter, info, opts)
            local field_type = field_type or
                bare_destination_type.fields[field_name].kind
            local is_list = core_types.nullable(field_type).__type == 'List'
            return result, is_list
        end
    end

    -- captures c.{type, parts, name, destination_collection}, collection_name,
    -- genResolveField, arguments, accessor
    return function(parent, args_instance, info, opts)
        local opts = opts or {}
        check(opts, 'opts', 'table')
        local is_hidden = opts.is_hidden or false
        check(is_hidden, 'is_hidden', 'boolean')

        local from = gen_from_parameter(collection_name, parent, c)

        -- Avoid non-needed index lookup on a destination collection when
        -- all connection parts are null:
        -- * return null for 1:1 connection;
        -- * return {} for 1:N connection (except the case when source
        --   collection is the query or the mutation pseudo-collection).
        if collection_name ~= nil and are_all_parts_null(parent, c.parts) then
            local objs = c.type == '1:N' and {} or nil
            if gen_prepare then
                return {
                    is_calculated = true,
                    objs = objs,
                    invoke = function()
                        -- error('internal error: should not be called') -- XXX: remove it?
                        return objs
                    end,
                }
            else
                return objs
            end
        end

        local exp_tuple_count
        if not disable_dangling_check and c.type == '1:1' then
            exp_tuple_count = 1
        end

        local resolveField = genResolveField(info)
        local extra = {
            qcontext = info.qcontext,
            resolveField = resolveField, -- for subrequests
            extra_args = {},
            exp_tuple_count = exp_tuple_count,
            is_hidden = opts.is_hidden,
        }

        -- object_args_instance will be passed to 'filter'
        -- list_args_instance will be passed to 'args'
        -- extra_args_instance will be passed to 'extra.extra_args'
        local arguments_instance = separate_args_instance(args_instance,
            arguments)
        extra.extra_args = arguments_instance.extra

        if gen_prepare then
            local prepared_select = accessor:prepare_select(parent,
                c.destination_collection, from,
                arguments_instance.object, arguments_instance.list, extra)
            return {
                is_calculated = false,
                prepared_select = prepared_select,
                -- opts = opts,
                accessor = accessor,
                connection = c,
                invoke = invoke_resolve,
            }
        else
            local objs = accessor:select(parent,
                c.destination_collection, from,
                arguments_instance.object, arguments_instance.list, extra)
            assert(type(objs) == 'table',
                'objs list received from an accessor ' ..
                'must be a table, got ' .. type(objs))
            if c.type == '1:1' then
                return objs[1] -- nil for empty list of matching objects
            else -- c.type == '1:N'
                return objs
            end
        end
    end
end

function resolve.gen_resolve_function_multihead(collection_name, connection,
        union_types, var_num_to_box_field_name, accessor, opts)
    local opts = opts or {}
    local disable_dangling_check = opts.disable_dangling_check or false
    local gen_prepare = opts.gen_prepare or false
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

    return function(parent, _, info, opts)
        local opts = opts or {}
        check(opts, 'opts', 'table')
        local is_hidden = opts.is_hidden or false
        check(is_hidden, 'is_hidden', 'boolean')

        -- If a parent object does not have all source fields (for any of
        -- variants) non-null then we do not resolve variant and just return
        -- box.NULL.
        local is_source_fields_found = false
        for _, variant in ipairs(c.variants) do
            is_source_fields_found =
                not are_all_parts_null(parent, variant.parts, {no_assert = true})
            if is_source_fields_found then
                break
            end
        end

        if not is_source_fields_found then
            if gen_prepare then
                return {
                    is_calculated = true,
                    objs = box.NULL,
                }
            else
                return box.NULL, nil
            end
        end

        local v, variant_num, box_field_name = resolve_variant(parent)
        local destination_type = union_types[variant_num]

        local quazi_connection = {
            type = c.type,
            parts = v.parts,
            name = c.name,
            destination_collection = v.destination_collection,
            index_name = v.index_name,
        }
        local gen_opts = {
            disable_dangling_check = disable_dangling_check,
            gen_prepare = gen_prepare,
        }
        -- XXX: generate a function (using gen_resolve_function) for each
        -- variant once at schema generation time
        if gen_prepare then
            local result = resolve.gen_resolve_function(collection_name,
                quazi_connection, destination_type, {}, accessor, gen_opts)(
                parent, {}, info, opts)
            result.connection = quazi_connection
            result.invoke = function(prepared_resolve)
                local result = invoke_resolve(prepared_resolve)
                -- see comment below
                return {[box_field_name] = result}, destination_type
            end
            return result
        else
            local result = resolve.gen_resolve_function(collection_name,
                quazi_connection, destination_type, {}, accessor, gen_opts)(
                parent, {}, info, opts)
            -- This 'wrapping' is needed because we use 'select' on 'collection'
            -- GraphQL type and the result of the resolve function must be in
            -- {'collection_name': {result}} format to be avro-valid.
            return {[box_field_name] = result}, destination_type
        end
    end
end

return resolve

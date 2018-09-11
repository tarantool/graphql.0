local avro_schema = require('avro_schema')


-- Generate virtbox.
-- Calls like virtbox.sname:mname(data...) would would call `mname`
-- this way: mname(vspace, data...)
-- @param space_methods Methods which are accessible for any space in a virtbox.
-- @param meta GraphQl_mata like table.
-- @param extra Extra data to be accessible in vspace.
local function new(space_methods, meta, extra)
    -- TODO: replace mata to space_names.
    local virtbox = {meta = meta, extra = extra}
    local space_mt = {
        __index = space_methods
    }
    local spaces = {}
    for sname, _ in pairs(meta.collections) do
        spaces[sname] = setmetatable({
            virtbox = virtbox,
            name = sname,
            meta = meta,
            extra = extra,
        }, space_mt)
    end
    local box_mt = {
        __index = spaces
    }
    return setmetatable(virtbox, box_mt)
end

-- Get a new virtbox, which add extra methods to a box-like object.
local function wrap_box(xbox, extra_methods, meta)
    local space_methods = setmetatable(extra_methods, {
        __index = function(self, fname)
            return function(vspace, ...)
                local space = xbox[vspace.name]
                assert(space, vspace.name)
                local method = space[fname]
                assert(method, fname)
                return method(space, ...)
            end
        end
    })
    return new(space_methods, meta, {xbox = xbox})
end

-- GQL specific methods.

-- Cache of compiled schemas:
-- {<meta> = {<schema_name> = <compiled_schema>}}
local models_cache = setmetatable({}, {__mode = "k"})

-- simplified version of the same named function from accessor_general.lua
local function compile_schemas(meta)
    local schemas = meta.schemas
    local service_fields = meta.service_fields
    local service_fields_types = {}
    for name, service_fields_list in pairs(service_fields) do
        local sf_types = {}
        local sf_defaults = {}
        for _, v in ipairs(service_fields_list) do
            sf_types[#sf_types + 1] = v.type
            sf_defaults[#sf_defaults + 1] = v.default
        end
        service_fields_types[name] = sf_types
    end

    local models = {}
    for name, schema in pairs(schemas) do
        local ok, handle = avro_schema.create(schema)
        assert(ok)
        local sf_types = service_fields_types[name]
        local ok, model = avro_schema.compile(
            {handle, service_fields = sf_types})
        assert(ok)
        models[name] = model
    end
    return models
end

local function get_model(meta, collection_name)
    local schema_name = meta.collections[collection_name].schema_name
    assert(schema_name ~= nil)
    if models_cache[meta] == nil then
        models_cache[meta] = compile_schemas(meta)
    end
    local model = models_cache[meta][schema_name]
    assert(model, ('Schema "%s" cannot be found'):format(schema_name))
    return model
end

local function flatten_object(meta, collection_name, object,
        service_field_values)
    local model = get_model(meta, collection_name)
    local ok, tuple = model.flatten(object, unpack(service_field_values or {}))
    assert(ok, ('flatten error: %s; collection %s'):format(tostring(tuple),
        collection_name))
    return tuple
end

local function unflatten_tuple(meta, collection_name, tuple)
    local model = get_model(meta, collection_name)
    local ok, object = model.unflatten(tuple)
    assert(ok, tostring(object))
    return object
end

local function parts_are_equal(parts_a, parts_b)
    if #parts_a ~= #parts_b then
        return false
    end
    for i, _ in ipairs(parts_a) do
        if parts_a[i] ~= parts_b[i] then
            return false
        end
    end
    return true
end

local function find_pk(indexes)
    for _, index in pairs(indexes) do
        if index.primary then
            return index
        end
    end
    error('pk not found')
end

local function pk_equal_to_vshard_key(meta, collection_name)
    local pk = find_pk(meta.indexes[collection_name])
    assert(not pk.service_fields or #pk.service_fields == 0)
    local pk_fields = pk.fields
    local vspace_cfg = meta.vshard[collection_name]
    assert(vspace_cfg.key_fields)
    return parts_are_equal(pk_fields, vspace_cfg.key_fields)
end

local function get_virtbox_for_accessor(xtype, ctx)
    local extra_methods = {
           get_object = function (vspace, key)
                local tuple = vspace.virtbox[vspace.name]:get(key)
                if tuple == nil then
                    return nil
                end
                return unflatten_tuple(vspace.meta, vspace.name, tuple)
            end,
        }
    if xtype == 'shard' or xtype == 'space' then
        extra_methods.replace_object = function (vspace, object, sf_values)
            local tuple = flatten_object(vspace.meta, vspace.name, object,
                sf_values)
            vspace:replace(tuple)
        end
        local xbox
        if xtype == 'shard' then
            xbox = ctx.shard
            -- Crutch: fetch all schemas before any operations shard:#81
            for _, zone in ipairs(xbox.shards) do
                for _, node in ipairs(zone) do
                    node.conn:ping()
                end
            end
            extra_methods.get = function(vspace, key)
                local shard = vspace.extra.xbox
                local tuples = shard:secondary_select(vspace.name, 0, nil, key)
                -- More than one tuple can be returned returned if tuple
                -- was replaced and pk != first_tuple_field.
                return tuples[1]
            end
            extra_methods.delete = function(vspace, key)
                local shard = vspace.extra.xbox
                assert(shard)
                -- Delete from all replicasets to decrease the
                -- replace=create_new effect (pk != first_tuple_field)
                for _, zone in ipairs(shard.shards) do
                    for _, node in ipairs(zone) do
                        assert(shard[vspace.name])
                        shard:space_call(vspace.name, node, function(space_obj)
                            space_obj:delete(key)
                        end)
                    end
                end
            end
        elseif xtype == 'space' then
            xbox = box.space
        end
        return wrap_box(xbox, extra_methods, ctx.meta)
    end
    assert(xtype == 'vshard')
    extra_methods.replace_object = function (vspace, object, sf_values)
        object = table.copy(object)
        local meta = vspace.meta
        local vshard_space_cfg = meta.vshard[vspace.name]
        local kf_values = {}
        for _, kf in ipairs(vshard_space_cfg.key_fields) do
            local f_value = object[kf]
            assert(f_value, kf)
            table.insert(kf_values, f_value)
        end
        object[vshard_space_cfg.bucket_id_field] =
            vshard_space_cfg.get_bucket_id(unpack(kf_values))
        local tuple = flatten_object(vspace.meta, vspace.name, object,
            sf_values)
        return vspace:replace(tuple)
    end
    extra_methods.replace = function(vspace, tuple)
        local meta = vspace.meta
        local router = vspace.extra.router
        local schema_name = meta.collections[vspace.name].schema_name
        local sf = meta.service_fields
        local bucket_fname = meta.vshard[vspace.name].bucket_id_field
        assert(bucket_fname)
        local bucket_id_fnum = #sf[schema_name]
        -- This procedure may not work well if complex types or avro-2 nullables
        -- are before the bucket_id.
        for i, field in ipairs(meta.schemas[schema_name].fields) do
            if field.name == bucket_fname then
                bucket_id_fnum = bucket_id_fnum + i
                break
            end
        end
        assert(bucket_id_fnum > #sf[schema_name])
        local bucket_id = tuple[bucket_id_fnum]
        local ret, err = router:callrw(bucket_id, 'space_call',
            {vspace.name, nil, 'replace', tuple})
        if ret == nil and err ~= nil then error(err) end
        return ret
    end
    extra_methods.get = function(vspace, key)
        local meta = vspace.meta
        assert(pk_equal_to_vshard_key(meta, vspace.name))
        local router = vspace.extra.router
        local vshard_space_cfg = meta.vshard[vspace.name]
        local bucket_id = vshard_space_cfg.get_bucket_id(unpack(key))
        local ret, err = router:callrw(bucket_id, 'space_call',
            {vspace.name, nil, 'get', key})
        if ret == nil and err ~= nil then error(err) end
        return ret
    end
    extra_methods.delete = function(vspace, key)
        local meta = vspace.meta
        assert(pk_equal_to_vshard_key(meta, vspace.name))
        local router = vspace.extra.router
        local vshard_space_cfg = meta.vshard[vspace.name]
        local bucket_id = vshard_space_cfg.get_bucket_id(unpack(key))
        local ret, err = router:callrw(bucket_id, 'space_call',
            {vspace.name, nil, 'delete', key})
        if ret == nil and err ~= nil then error(err) end
        return ret
    end
    return new(extra_methods, ctx.meta, {router = ctx.router})
end

return {
    new = new,
    wrap_box = wrap_box,
    get_virtbox_for_accessor = get_virtbox_for_accessor,
}

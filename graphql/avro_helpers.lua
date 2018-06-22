--- The module is collection of helpers to simplify avro-schema related tasks.

local json = require('json')
local avro_schema = require('avro_schema')

local utils = require('graphql.utils')
local check = utils.check

local avro_helpers = {}

--- The function converts avro type to the corresponding nullable type in
--- place and returns the result.
---
--- We make changes in place in case of table input (`avro`) because of
--- performance reasons, but we returns the result because an input (`avro`)
--- can be a string. Strings in Lua are immutable.
---
--- In the current tarantool/avro-schema implementation we simply add '*' to
--- the end of a type name or, in case of union, add 'null' branch.
---
--- If the type is already nullable the function leaves it as is if
--- `opts.raise_on_nullable` is false or omitted. If `opts.raise_on_nullable`
--- is true the function will raise an error.
---
--- @tparam table avro avro schema node to be converted to nullable one
---
--- @tparam[opt] table opts the following options:
---
--- * `raise_on_nullable` (boolean) raise an error on nullable type
---
--- @return `result` (string or table) nullable avro type
function avro_helpers.make_avro_type_nullable(avro, opts)
    assert(avro ~= nil, "avro must not be nil")
    local opts = opts or {}
    assert(type(opts) == 'table',
        'opts must be nil or a table, got ' .. type(opts))
    local raise_on_nullable = opts.raise_on_nullable or false
    assert(type(raise_on_nullable) == 'boolean',
        'opts.raise_on_nullable must be nil or a boolean, got ' ..
        type(raise_on_nullable))

    local value_type = type(avro)

    if value_type == "string" then
        local is_nullable = avro:endswith("*")
        if raise_on_nullable and is_nullable then
            error('expected non-null type, got the nullable one: ' ..
                json.encode(avro))
        end
        return is_nullable and avro or (avro .. '*')
    elseif value_type == 'table' and #avro > 0 then -- union
        local is_nullable = false
        for _, branch in ipairs(avro) do
            if branch == 'null' then
                is_nullable = true
                break
            end
        end
        if raise_on_nullable and is_nullable then
            error('expected non-null type, got the nullable one: ' ..
                json.encode(avro))
        end
        -- We add 'nil' branch to the end because here we don't know whether
        -- the following things matter for a caller:
        -- * a default value (it must have the type of the first branch);
        -- * {,un,x}flatten data layout.
        if not is_nullable then
            table.insert(avro, 'null')
        end
        return avro
    elseif value_type == 'table' and #avro == 0 then
        avro.type = avro_helpers.make_avro_type_nullable(avro.type, opts)
        return avro
    end

    error("avro should be a string or a table, got " .. value_type)
end

--- Determines major version of the avro-schema module in use.
---
--- @treturn number 2 or 3
function avro_helpers.major_avro_schema_version()
    local ok, handle = avro_schema.create('boolean')
    assert(ok, tostring(handle))
    local ok, model = avro_schema.compile(handle)
    assert(ok, tostring(model))
    return model.get_types == nil and 2 or 3
end

function avro_helpers.is_scalar_type(avro_schema_type)
    check(avro_schema_type, 'avro_schema_type', 'string')

    local scalar_types = {
        ['int'] = true,
        ['int*'] = true,
        ['long'] = true,
        ['long*'] = true,
        ['float'] = true,
        ['float*'] = true,
        ['double'] = true,
        ['double*'] = true,
        ['boolean'] = true,
        ['boolean*'] = true,
        ['string'] = true,
        ['string*'] = true,
        ['null'] = true,
    }

    return scalar_types[avro_schema_type] or false
end

function avro_helpers.is_compound_type(avro_schema_type)
    check(avro_schema_type, 'avro_schema_type', 'string')

    local compound_types = {
        ['record'] = true,
        ['record*'] = true,
        ['array'] = true,
        ['array*'] = true,
        ['map'] = true,
        ['map*'] = true,
        ['union'] = true,
    }

    return compound_types[avro_schema_type] or false
end

--- Get type of an avro-schema.
---
--- @param avro_schema (table or string) input avro-schema
---
--- @tparam[opt] table opts the following options:
---
--- * allow_references (boolean)
---
--- @treturn string `avro_t` type of the avro-schema
---
--- @treturn boolean `is_ref` whether the avro-schema is reference to another
--- avro-schema type
function avro_helpers.avro_type(avro_schema, opts)
    local opts = opts or {}
    local allow_references = opts.allow_references or false

    if type(avro_schema) == 'table' then
        if utils.is_array(avro_schema) then
            return 'union', false
        elseif avro_helpers.is_compound_type(avro_schema.type) then
            return avro_schema.type, false
        elseif allow_references then
            return avro_schema, true
        end
    elseif type(avro_schema) == 'string' then
        if avro_helpers.is_scalar_type(avro_schema) then
            return avro_schema, false
        elseif allow_references then
            return avro_schema, true
        end
    end

    error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
end

--- Expand avro-schema references.
---
--- @param avro_schema (table or string) input avro-schema
---
--- @tparam[opt] table opts the following options:
---
--- * definitions (table) processed avro-schemas to expand further references
---
--- @return generated expanded avro-schema
function avro_helpers.expand_references(avro_schema, opts)
    local opts = opts or {}
    local definitions = opts.definitions or {}

    local avro_t, is_ref = avro_helpers.avro_type(avro_schema,
        {allow_references = true})

    if is_ref then
        assert(definitions[avro_t] ~= nil,
            ('undefined reference: %s'):format(avro_t))
        return definitions[avro_t]
    elseif avro_t == 'union' then
        local res = {}
        for _, child in ipairs(avro_schema) do
            table.insert(res, avro_helpers.expand_references(child,
                {definitions = definitions}))
        end
        return res
    elseif avro_t == 'record' or avro_t == 'record*' then
        local res = table.copy(avro_schema)
        res.fields = {}

        local res_nonnull
        local res_nullable
        if avro_t == 'record' then
            res_nonnull = res
            res_nullable = table.copy(res)
            res_nullable.type = 'record*'
            res_nullable.fields = res.fields
        else
            res_nonnull = table.copy(res)
            res_nonnull.type = 'record'
            res_nonnull.fields = res.fields
            res_nullable = res
        end

        -- Saving type before traverse deeper allows to use reference to it
        -- inside (it is allowed by our avro-schema implementation for nullable
        -- fields, union, array and map).
        local name = avro_schema.name
        assert(definitions[name] == nil and definitions[name .. '*'] == nil,
            ('multiple definitions of %s'):format(name))
        definitions[name] = res_nonnull
        definitions[name .. '*'] = res_nullable

        for _, field in ipairs(avro_schema.fields) do
            local field = table.copy(field)
            field.type = avro_helpers.expand_references(field.type,
                {definitions = definitions})
            table.insert(res.fields, field)
        end

        return res
    elseif avro_t == 'array' or avro_t == 'array*' then
        local res = table.copy(avro_schema)
        res.items = avro_helpers.expand_references(avro_schema.items,
            {definitions = definitions})
        return res
    elseif avro_t == 'map' or avro_t == 'map*' then
        local res = table.copy(avro_schema)
        res.values = avro_helpers.expand_references(avro_schema.values,
            {definitions = definitions})
        return res
    elseif avro_helpers.is_scalar_type(avro_t) then
        return avro_schema
    end

    error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
end

return avro_helpers

--- The module us collection of helpers to simplify avro-schema related tasks.

local json = require('json')

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
--- @result `result` (string or table) nullable avro type
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
        return avro_helpers.make_avro_type_nullable(avro.type, opts)
    end

    error("avro should be a string or a table, got " .. value_type)
end

return avro_helpers

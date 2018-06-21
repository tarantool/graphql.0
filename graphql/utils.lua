--- Various utility function used across the graphql module sources and tests.

local json = require('json')
local log = require('log')
local ffi = require('ffi')

local utils = {}

--- Return an error w/o file name and line number.
function utils.strip_error(err)
    local res = tostring(err):gsub('^.-:.-: (.*)$', '%1')
    return res
end

--- Recursively checks whether `sub` fields values are match `t` ones.
function utils.is_subtable(t, sub)
    for k, v in pairs(sub) do
        if type(v) == 'table' then
            if t[k] == nil then
                return false
            end
            if not utils.is_subtable(t[k], v) then
                return false
            end
        elseif t[k] ~= v then
            return false
        end
    end
    return true
end

--- Check whether table is an array.
---
--- Based on [that][1] implementation.
--- [1]: https://github.com/mpx/lua-cjson/blob/db122676/lua/cjson/util.lua
---
--- @tparam table table to check
--- @return[1] `true` if passed table is an array (includes the empty table
--- case)
--- @return[2] `false` otherwise
function utils.is_array(table)
    if type(table) ~= 'table' then
        return false
    end
    local max = 0
    local count = 0
    for k, _ in pairs(table) do
        if type(k) == 'number' then
            if k > max then
                max = k
            end
            count = count + 1
        else
            return false
        end
    end
    if max > count * 2 then
        return false
    end

    return max >= 0
end

--- Creates a table containing fields from all passed tables.
---
--- When the field names intersects between passed tables a value from a latter
--- table have precedence.
function utils.merge_tables(...)
    local res = {}
    for i = 1, select('#', ...) do
        local t = select(i, ...)
        for k, v in pairs(t) do
            res[k] = v
        end
    end
    return res
end

local rawpairs = pairs

--- Replacement for global pairs function to call __pairs() if it exists.
function pairs(table) -- luacheck: ignore
    local mt = getmetatable(table)
    local p = mt and mt.__pairs or rawpairs
    return p(table)
end

--- Generate an object that behaves like a table stores another tables as
--- values and always returns the same table (the same reference) as a value.
--- It performs copying of a value fields instead of assigning and returns an
--- empty table for fields that not yet exists. Such approach helps with
--- referencing a table that will be filled later.
---
--- @tparam table data the initial values
function utils.gen_booking_table(data)
    assert(type(data) == 'table',
        'initial data must be a table, got ' .. type(data))
    return setmetatable({data = data}, {
        __index = function(table, key)
            local data = rawget(table, 'data')
            if data[key] == nil then
                data[key] = {}
            end
            return data[key]
        end,
        __newindex = function(table, key, value)
            assert(type(value) == 'table',
                'value to set must be a table, got ' .. type(value))
            local data = rawget(table, 'data')
            if data[key] == nil then
                data[key] = {}
            end
            for k, _ in pairs(data[key]) do
                data[key][k] = nil
            end
            assert(next(data[key]) == nil,
                ('data[%s] must be nil, got %s'):format(tostring(key),
                tostring(next(data[key]))))
            for k, v in pairs(value) do
                data[key][k] = v
            end
        end,
        __pairs = function(table)
            local data = rawget(table, 'data')
            return rawpairs(data)
        end,
    })
end

--- Catch error at module require and return nil in the case.
---
--- @tparam string module_name mane of a module to require
---
--- @return `module` or `nil`
function utils.optional_require(module_name)
    assert(type(module_name) == 'string',
        'module_name must be a string, got ' .. type(module_name))
    local ok, module = pcall(require, module_name)
    if not ok then
        log.warn('optional_require: no module ' .. module_name)
    end
    return ok and module or nil
end

--- @return `table` with all keys of the given table
function utils.get_keys(table)
    local keys = {}
    for k, _ in pairs(table) do
        keys[#keys + 1] = k
    end
    return keys
end

--- Check if passed table has passed keys with non-nil values.
--- @tparam table table to check
--- @tparam table keys array of keys to check
--- @return[1] `true` if passed table has passed keys
--- @return[2] `false` otherwise
function utils.do_have_keys(table, keys)
    for _, k in pairs(keys) do
        if table[k] == nil then
            return false
        end
    end
    return true
end

--- Check whether passed value has one of listed types.
---
--- @param obj value to check
---
--- @tparam string obj_name name of the value to form an error
---
--- @tparam string type_1
--- @tparam[opt] string type_2
--- @tparam[opt] string type_3
---
--- @return nothing
function utils.check(obj, obj_name, type_1, type_2, type_3)
    if type(obj) == type_1 or type(obj) == type_2 or type(obj) == type_3 then
        return
    end

    if type_3 ~= nil then
        error(('%s must be a %s or a % or a %, got %s'):format(obj_name, type_1,
            type_2, type_3, type(obj)))
    elseif type_2 ~= nil then
        error(('%s must be a %s or a %, got %s'):format(obj_name, type_1,
        type_2, type(obj)))
    else
        error(('%s must be a %s, got %s'):format(obj_name, type_1, type(obj)))
    end
end

--- Check if given table has only one specific key.
function utils.has_only(t, key)
    local fst_key = next(t)
    local snd_key = next(t, fst_key)
    return fst_key == key and snd_key == nil
end

function utils.table_size(t)
    local count = 0
    for _, _ in pairs(t) do
        count = count + 1
    end
    return count
end

function utils.value_in(value, array)
    for _, v in ipairs(array) do
        if value == v then
            return true
        end
    end
    return false
end

function utils.optional_require_rex()
    local rex, is_pcre2 = utils.optional_require('rex_pcre2'), true
    if rex == nil then
        -- fallback to libpcre
        rex, is_pcre2 = utils.optional_require('rex_pcre'), false
    end
    return rex, is_pcre2
end

function utils.serialize_error(err, traceback)
    if type(err) == 'string' then
        return {
            message = utils.strip_error(err),
            traceback = traceback,
        }
    elseif type(err) == 'cdata' and
            tostring(ffi.typeof(err)) == 'ctype<const struct error &>' then
        return {
            message = tostring(err),
            traceback = traceback,
        }
    elseif type(err) == 'table' and type(err.message) == 'string' then
        local err = table.copy(err)
        err.traceback = traceback
        return err
    end

    local message = 'internal error: unknown error format'
    local encode_use_tostring_orig = json.cfg.encode_use_tostring
    json.cfg({encode_use_tostring = true})
    local orig_error = json.encode(err)
    json.cfg({encode_use_tostring = encode_use_tostring_orig})
    return {
        message = message,
        orig_error = orig_error,
        traceback = traceback,
    }
end

return utils

local json = require('json')

local parse = require('graphql.parse')
local schema = require('graphql.schema')
local types = require('graphql.types')
local validate = require('graphql.validate')
local execute = require('graphql.execute')

local tarantool_graphql = {}

-- forward declarations
local gql_type

-- XXX: don't touch global options
-- global configuration
require('yaml').cfg{encode_use_tostring = true}
require('json').cfg{encode_use_tostring = true}

--- Check whether table is an array.
--- Based on [that][1].
--- [1]: https://github.com/mpx/lua-cjson/blob/db122676/lua/cjson/util.lua
--- @param table
--- @return True if table is an array
local function is_array(table)
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

local function avro_type(avro_schema)
    if type(avro_schema) == 'table' and avro_schema.type == 'record' then
        return 'record'
    elseif type(avro_schema) == 'table' and is_array(avro_schema) then
        return 'enum'
    elseif type(avro_schema) == 'string' and avro_schema == 'int' then
        return 'int'
    elseif type(avro_schema) == 'string' and avro_schema == 'long' then
        return 'long'
    elseif type(avro_schema) == 'string' and avro_schema == 'string' then
        return 'string'
    else
        error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
    end
end

local function nullable(gql_class)
    assert(type(gql_class) == 'table', 'gql_class must be a table, got ' ..
        type(gql_class))

    if gql_class.__type ~= 'NonNull' then return gql_class end

    assert(gql_class.ofType ~= nil, 'gql_class.ofType must not be nil')
    return gql_class.ofType
end

local function convert_record_fields(state_for_read, fields)
    local res = {}
    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string',
            ('field.name must be a string, got %s (schema %s)')
            :format(type(field.name), json.encode(field)))
        res[field.name] = {
            name = field.name,
            kind = gql_type(state_for_read, field.type),
        }
        args[field.name] = nullable(res[field.name].kind)
    end
    return res, args
end

local types_long = types.scalar({
    name = 'Long',
    description = 'Long is non-bounded integral type',
    serialize = function(value) return tonumber(value) end,
    parseValue = function(value) return tonumber(value) end,
    parseLiteral = function(node)
        if node.kind == 'long' then -- XXX: from where this 'long' we can get?
            return tonumber(node.value)
        end
    end
})

-- XXX: types that are not yet exists in state_for_read (but will be later,
-- like a circular link)
-- XXX: assert for name match with schema.name
-- storage argument is optional
gql_type = function(state_for_read, avro_schema, storage)
    local state_for_read = state_for_read or {}
    assert(type(state_for_read) == 'table',
        'state_for_read must be a table or nil, got ' ..
        type(state_for_read))
    -- XXX: assert for state_for_read.accessor
    local accessor = state_for_read.accessor

    if avro_type(avro_schema) == 'record' then
        assert(type(avro_schema.name) == 'string',
            ('avro_schema.name must be a string, got %s (avro_schema %s)')
            :format(type(avro_schema.name), json.encode(avro_schema)))
        assert(type(avro_schema.fields) == 'table',
            ('avro_schema.fields must be a table, got %s (avro_schema %s)')
            :format(type(avro_schema.fields), json.encode(avro_schema)))

        local fields, args = convert_record_fields(state_for_read,
            avro_schema.fields)

        -- XXX: think re 1:N connections: in that case type must be a list
        for _, c in ipairs((storage or {}).connections or {}) do
            local destination_type =
                state_for_read.types[c.destination_storage]
            fields[c.destination_storage] = {
                name = c.destination_storage,
                kind = destination_type,
                resolve = function(parent, args, info)
                    local args = table.copy(args) -- luacheck: ignore
                    for _, bind in ipairs(c.parts) do
                        args[bind.destination_field] =
                            parent[bind.source_field]
                    end
                    return accessor:get(parent, c.destination_storage, args)
                end,
            }
        end

        local res = types.nonNull(types.object({
            name = storage ~= nil and storage.name or avro_schema.name,
            description = 'generated from avro-schema for ' ..
                avro_schema.name,
            fields = fields,
        }))
        -- XXX: add limit, offset, filter

       return res, args, avro_schema.name
    elseif avro_type(avro_schema) == 'enum' then
        error('enums not implemented yet') -- XXX
    elseif avro_type(avro_schema) == 'int' then
        return types.int.nonNull
    elseif avro_type(avro_schema) == 'long' then
        return types_long.nonNull
    elseif avro_type(avro_schema) == 'string' then
        return types.string.nonNull
    else
        error('unrecognized avro-schema type: ' .. json.encode(avro_schema))
    end
end

-- XXX: asserts for types
local function parse_cfg(cfg)
    local state = {}
    state.types = {}
    state.arguments = {}
    local accessor = cfg.accessor
    state.accessor = accessor
    local storage = table.copy(cfg.storage) -- luacheck: ignore
    state.storage = storage

    local fields = {}

    for name, storage in pairs(cfg.storages) do
        storage.name = name
        assert(storage.type ~= nil, 'storage.type must not be nil')
        local schema = cfg.schemas[storage.type]
        assert(schema ~= nil, ('cfg.schemas[%s] must not be nil'):format(
            tostring(storage.type)))
        local schema_name
        state.types[name], state.arguments[name], schema_name =
            gql_type(state, schema, storage)
        assert(schema_name == nil or schema_name == storage.type,
            ('top-level schema name does not match the name in ' ..
            'the schema itself: "%s" vs "%s"'):format(storage.type,
            schema_name))

        -- create entry points from storage names
        fields[name] = {
            kind = types.nonNull(types.list(state.types[name])),
            arguments = state.arguments[name],
            resolve = function(rootValue, args, info)
                return accessor:select(rootValue, name, args)
            end,
        }
    end

    local schema = schema.create({
        query = types.object({
            name = 'Query',
            fields = fields,
        })
    })
    state.schema = schema

    return state
end

local function gql_execute(qstate, variables)
    assert(qstate.state)
    local state = qstate.state
    assert(state.schema)

    assert(type(variables) == 'table', 'variables must be table, got ' ..
        type(variables))

    local root_value = {}
    local operation_name = 'obtainOrganizationUsers' -- XXX: qstate. ...

    return execute(state.schema, qstate.ast, root_value, variables,
        operation_name)
end

local function gql_compile(state, query)
    assert(type(state) == 'table' and type(query) == 'string',
        'use :validate(...) instead of .validate(...)')
    assert(state.schema ~= nil, 'have not compiled schema')

    local ast = parse(query)
    validate(state.schema, ast)

    local qstate = {
        state = state,
        ast = ast,
    }
    local gql_query = setmetatable(qstate, {
        __index = {
            execute = gql_execute,
        }
    })
    return gql_query
end

function tarantool_graphql.new(cfg)
    local state = parse_cfg(cfg)
    return setmetatable(state, {
        __index = {
            compile = gql_compile,
        }
    })
end

return tarantool_graphql

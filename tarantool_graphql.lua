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
--- [1]: https://github.com/mpx/lua-cjson/blob/db12267686af80f0a3643897d09016c137715c8a/lua/cjson/util.lua
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

local function avro_type(schema)
    if type(schema) == 'table' and schema.type == 'record' then
        return 'record'
    elseif type(schema) == 'table' and is_array(schema) then
        return 'enum'
    elseif type(schema) == 'string' and schema == 'int' then
        return 'int'
    elseif type(schema) == 'string' and schema == 'long' then
        return 'long'
    elseif type(schema) == 'string' and schema == 'string' then
        return 'string'
    else
        error('unrecognized avro-schema type: ' .. json.encode(schema))
    end
end

local function nullable(gql_class)
    assert(type(gql_class) == 'table', 'gql_class must be a table, got ' ..
        type(gql_class))

    if gql_class.__type ~= 'NonNull' then return gql_class end

    assert(gql_class.ofType ~= nil, 'gql_class.ofType must not be nil')
    return gql_class.ofType
end

local function convert_record_fields(fields, state_for_read)
    local res = {}
    local args = {}
    for _, field in ipairs(fields) do
        assert(type(field.name) == 'string', ('field.name must be a string, ' ..
            'got %s (schema %s)'):format(type(field.name), json.encode(field)))
        res[field.name] = {
            name = field.name,
            kind = gql_type(field.type, state_for_read),
        }
        args[field.name] = nullable(res[field.name].kind)
    end
    return res, args
end

local function get_connection_map(schema)
    local connection_map = {}
    local connections = schema.connection or {}
    for _, c in ipairs(connections) do
        assert(type(c.name) == 'string',
            'connection.name must be a string, got ' .. type(c.name))
        -- XXX: more asserts
        connection_map[c.name] = {
            source_storage = c.name, -- XXX: does this mtach the storage name?
            destination_storage = c.destination,
            source_field = c.parts[1].source,
            destination_field = c.parts[1].destination,
        }
    end
    return connection_map
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
gql_type = function(schema, state_for_read)
    local state_for_read = state_for_read or {}
    assert(type(state_for_read) == 'table',
        'state_for_read must be a table or nil, got ' ..
        type(state_for_read))
    -- XXX: assert for state_for_read.accessor
    local accessor = state_for_read.accessor

    if avro_type(schema) == 'record' then
        assert(type(schema.name) == 'string', ('schema.name must be a string, ' ..
            'got %s (schema %s)'):format(type(schema.name), json.encode(schema)))
        assert(type(schema.fields) == 'table', ('schema.fields must be a table, ' ..
            'got %s (schema %s)'):format(type(schema.fields), json.encode(schema)))

        local connection_map = get_connection_map(schema)
        -- XXX: save storage + field_name -> storage + field_name mapping
        -- XXX: save storage names for use as query root

        local fields, args = convert_record_fields(schema.fields, state_for_read)
        for name, c in pairs(connection_map) do
            local gql_class = state_for_read.types[c.destination_storage]
            fields[c.destination_storage] = {
                name = c.destination_storage,
                kind = gql_class,
                resolve = function(rootValue, args, info)
                    local args = table.copy(args) -- luacheck: ignore
                    args[c.destination_field] = rootValue[c.source_field]
                    return accessor:get(rootValue, name, args)
                end,
            }
        end

        local res = types.nonNull(types.object({
            name = schema.name,
            description = 'generated from avro-schema for ' .. schema.name,
            fields = fields,
        }))
        -- XXX: add limit, offset, filter

       return res, args, schema.name
    elseif avro_type(schema) == 'enum' then
        error('enums not implemented yet') -- XXX
    elseif avro_type(schema) == 'int' then
        return types.int.nonNull
    elseif avro_type(schema) == 'long' then
        return types_long.nonNull
    elseif avro_type(schema) == 'string' then
        return types.string.nonNull
    else
        error('unrecognized avro-schema type: ' .. json.encode(schema))
    end
end

-- XXX: asserts for types
local function parse_cfg(cfg)
    local state = {}
    state.types = {}
    state.arguments = {}
    local accessor = cfg.accessor
    state.accessor = accessor

    for name, schema in pairs(cfg.schemas) do
        --print('DEBUG: ' .. '--------')
        --print('DEBUG: ' .. ('avro_type [%s]: %s'):format(name, require('yaml').encode(schema)))
        --print('DEBUG: ' .. '--------')
        local schema_name
        state.types[name], state.arguments[name], schema_name =
            gql_type(schema, state)
        assert(schema_name == nil or schema_name == name,
            ('top-level schema name does not match the name in ' ..
            'the schema itself: "%s" vs "%s"'):format(name, schema_name))
        --print('DEBUG: ' .. ('gql_type [%s]: %s'):format(name, require('yaml').encode(state.types[name])))
        --print('DEBUG: ' .. '--------')
        --print('DEBUG: ' .. ('arguments [%s]: %s'):format(name, require('yaml').encode(state.arguments[name])))
        --print('DEBUG: ' .. '--------')
    end

    -- XXX: entry points must be storages, not types
    local fields = {}
    for tname, tvalue in pairs(state.types) do
        fields[tname] = {
            kind = types.nonNull(types.list(tvalue)),
            arguments = state.arguments[tname],
            resolve = function(rootValue, args, info)
                return accessor:select(rootValue, tname, args)
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

    return execute(state.schema, qstate.ast, root_value, variables, operation_name)
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

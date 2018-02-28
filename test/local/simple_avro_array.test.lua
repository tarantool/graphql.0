#!/usr/bin/env tarantool

local fio = require('fio')

 --require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local json = require('json')
local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')

local schemas = json.decode([[{
    "user": {
        "name": "user",
        "type": "record",
        "fields": [
            { "name": "user_id", "type": "string" },
            { "name": "favorite_food", "type": {"type": "array", "items": "string"} }
        ]
    }
}]])

local collections = json.decode([[{
    "user_collection": {
        "schema_name": "user"
    }
}]])

local function simple_access_function(parent, collection_name, filter, args)
    --[[
    print('DEBUG: collection_name: ' .. collection_name)
    print('DEBUG: filter: ' .. json.encode(filter))
    print('DEBUG: args: ' .. json.encode(args))
    print('DEBUG: --------')
    --]]
    local obj
    if collection_name == 'user_collection' then
        obj = {
            user_id = 'def',
            favorite_food = { 'meat', 'potato' },
        }
    else
        error('NIY: ' .. collection_name)
    end
    if not utils.is_subtable(obj, filter) then
        return {}
    end
    return { obj }
end

local simple_accessor = setmetatable({}, {
    __index = {
        select = function(self, parent, collection_name, connection_name,
                          filter, args)
            return simple_access_function(parent, collection_name, filter, args)
        end,
        list_args = function(self, connection_type)
            if connection_type == '1:1' then
                return {}
            end
            return {
                { name = 'limit', type = 'int' },
                { name = 'offset', type = 'long' },
            -- {name = 'filter', type = ...},
            }
        end,
    }
})

local gql_wrapper_simple_accessor = graphql.new({
    -- class_name:class mapping
    schemas = schemas,
    -- collection_{schema_name=..., connections=...} mapping
    collections = collections,
    -- :select() and :list_args() provider
    accessor = simple_accessor,
})

local query_with_list = [[
    query userFavs($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            favorite_food
        }
    }
]]

utils.show_trace(function()
    local variables_2 = { user_id = 'def' }
    local gql_query_2 = gql_wrapper_simple_accessor:compile(query_with_list)
    local result = gql_query_2:execute(variables_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

os.exit()

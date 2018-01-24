#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local json = require('json')
local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')

local schemas = json.decode([[{
    "individual": {
        "name": "individual",
        "type": "record",
        "fields": [
            { "name": "individual_id", "type": "string" },
            { "name": "last_name", "type": "string" },
            { "name": "first_name", "type": "string" },
            { "name": "birthdate", "type": "string", "default": "" }
        ]
    },
    "organization": {
        "name": "organization",
        "type": "record",
        "fields": [
            { "name": "organization_id", "type": "string" },
            { "name": "organization_name", "type": "string" }
        ]
    },
    "relation": {
        "name": "relation",
        "type": "record",
        "fields": [
            { "name": "id", "type": "string" },
            { "name": "type", "type": "string" },
            { "name": "size", "type": "int" },
            { "name": "individual_id", "type": "string", "default": "" },
            { "name": "organization_id", "type": "string", "default": "" }
        ]
    },
    "user": {
        "type": "record",
        "name": "user",
        "fields": [
            { "name": "user_id", "type": "string" },
            { "name": "first_name", "type": "string" },
            { "name": "last_name", "type": "string" }
        ]
    },
    "order": {
        "type": "record",
        "name": "order",
        "fields": [
            { "name": "order_id", "type": "string" },
            { "name": "user_id", "type": "string" },
            { "name": "description", "type": "string" }
        ]
    }
}]])

local collections = json.decode([[{
    "individual_collection": {
        "schema_name": "individual"
    },
    "organization_collection": {
        "schema_name": "organization"
    },
    "relation_collection": {
        "schema_name": "relation",
        "connections": [
            {
                "type": "1:1",
                "name": "individual_connection",
                "destination_collection": "individual_collection",
                "parts": [
                    { "source_field": "individual_id", "destination_field": "individual_id" }
                ]
            },
            {
                "type": "1:1",
                "name": "organization_connection",
                "destination_collection": "organization_collection",
                "parts": [
                    { "source_field": "organization_id", "destination_field": "organization_id" }
                ]
            }
        ]
    },
    "user_collection": {
        "schema_name": "user",
        "connections": [
            {
                "type": "1:N",
                "name": "order_connection",
                "destination_collection":  "order_collection",
                "parts": [
                    { "source_field": "user_id", "destination_field": "user_id" }
                ]
            }
        ]
    },
    "order_collection": {
        "schema_name": "order",
        "connections": [
            {
                "type": "1:1",
                "name": "user_connection",
                "destination_collection":  "user_collection",
                "parts": [
                    { "source_field": "user_id", "destination_field": "user_id" }
                ]
            }
        ]
    }
}]])

local function access_function(parent, collection_name, filter, args)
    --[[
    print('DEBUG: collection_name: ' .. collection_name)
    print('DEBUG: filter: ' .. json.encode(filter))
    print('DEBUG: args: ' .. json.encode(args))
    print('DEBUG: --------')
    --]]
    local obj
    if collection_name == 'relation_collection' then
        obj = {
            id = 'abc',
            type = 'type 1',
            size = 2,
            individual_id = 'def',
            organization_id = 'ghi',
        }
    elseif collection_name == 'individual_collection' then
        obj = {
            individual_id = 'def',
            last_name = 'last name',
            first_name = 'first name',
            birthdate = '1970-01-01',
        }
    elseif collection_name == 'organization_collection' then
        obj = {
            organization_id = 'def',
            organization_name = 'qwqw',
        }
    elseif collection_name == 'user_collection' then
        obj = {
            user_id = 'def',
            last_name = 'last name',
            first_name = 'first name',
        }
    elseif collection_name == 'order_collection' then
        obj = {
            order_id = '123',
            user_id = 'def',
            description = 'the order 123',
        }
    else
        error('NIY: ' .. collection_name)
    end
    if not utils.is_subtable(obj, filter) then return {} end
    return {obj}
end

local accessor = setmetatable({}, {
    __index = {
        select = function(self, parent, collection_name, connection_name,
                filter, args)
            return access_function(parent, collection_name, filter, args)
        end,
        arguments = function(self, connection_type)
            if connection_type == '1:1' then return {} end
            return {
                {name = 'limit', type = 'int'},
                {name = 'offset', type = 'long'},
                -- {name = 'filter', type = ...},
            }
        end,
    }
})

local gql_wrapper = graphql.new({
    -- class_name:class mapping
    schemas = schemas,
    -- collection_{schema_name=..., connections=...} mapping
    collections = collections,
    -- :select() and :arguments() provider
    accessor = accessor,
})

local query_1 = [[
    query obtainOrganizationUsers($organization_id: String) {
        relation_collection(organization_id: $organization_id, type: "type 1", size: 2) {
            id
            individual_connection {
                last_name,
                first_name,
            }
        }
    }
]]

utils.show_trace(function()
    local variables_1 = {organization_id = 'ghi'}
    local gql_query_1 = gql_wrapper:compile(query_1)
    local result = gql_query_1:execute(variables_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

local query_2 = [[
    query user_order($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            last_name
            first_name
            order_connection(limit: 1, offset: 0) {
                order_id
                description
            }
        }
    }
]]

utils.show_trace(function()
    local variables_2 = {user_id = 'def'}
    local gql_query_2 = gql_wrapper:compile(query_2)
    local result = gql_query_2:execute(variables_2)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

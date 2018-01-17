#!/usr/bin/env tarantool

local json = require('json')
local tarantool_graphql = require('tarantool_graphql')

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
                "name": "individual_connection",
                "destination_collection": "individual_collection",
                "parts": [
                    { "source_field": "individual_id", "destination_field": "individual_id" }
                ]
            },
            {
                "name": "organization_connection",
                "destination_collection": "organization_collection",
                "parts": [
                    { "source_field": "organization_id", "destination_field": "organization_id" }
                ]
            }
        ]
    }
}]])

local function gen_access_function(collection_name, opts)
    local is_list = opts.is_list
    return function(rootValue, args, info)
        local obj
        if collection_name == 'relation_collection' then
            obj = {
                id = 'abc',
                type = '123',
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
        else
            error('NIY: ' .. collection_name)
        end
        return is_list and {obj} or obj
    end
end

local accessor = setmetatable({}, {
    __index = {
        get = function(self, parent, name, args)
            return gen_access_function(name, {is_list = false})(parent, args)
        end,
        select = function(self, parent, name, args)
            return gen_access_function(name, {is_list = true})(parent, args)
        end,
    }
})

local gql_wrapper = tarantool_graphql.new({
    -- class_name:class mapping
    schemas = schemas,
    -- collection_{schema_name=..., connections=...} mapping
    collections = collections,
    -- :get() and :select() provider
    accessor = accessor,
})

local query = [[
    query obtainOrganizationUsers($organization_id: String) {
        relation_collection(organization_id: $organization_id, type: "type 1", size: 2) {
            id
            individual_collection {
                last_name,
                first_name,
            }
        }
    }
]]

local variables = {organization_id = 1}
local gql_query_1 = gql_wrapper:compile(query)
local yaml = require('yaml')
local result = gql_query_1:execute(variables)
print(('RESULT\n%s'):format(yaml.encode(result)))

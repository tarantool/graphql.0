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

local storages = json.decode([[{
    "individual_st": {
        "schema_name": "individual"
    },
    "organization_st": {
        "schema_name": "organization"
    },
    "relation_st": {
        "schema_name": "relation",
        "connections": [
            {
                "name": "individual_connection",
                "destination_storage": "individual_st",
                "parts": [
                    { "source_field": "individual_id", "destination_field": "individual_id" }
                ]
            },
            {
                "name": "organization_connection",
                "destination_storage": "organization_st",
                "parts": [
                    { "source_field": "organization_id", "destination_field": "organization_id" }
                ]
            }
        ]
    }
}]])

local function gen_access_function(storage_name, opts)
    local is_list = opts.is_list
    return function(rootValue, args, info)
        local obj
        if storage_name == 'relation_st' then
            obj = {
                id = 'abc',
                type = '123',
                size = 2,
                individual_id = 'def',
                organization_id = 'ghi',
            }
        elseif storage_name == 'individual_st' then
            obj = {
                individual_id = 'def',
                last_name = 'last name',
                first_name = 'first name',
                birthdate = '1970-01-01',
            }
        elseif storage_name == 'organization_st' then
            obj = {
                organization_id = 'def',
                organization_name = 'qwqw',
            }
        else
            error('NIY: ' .. storage_name)
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
    -- storage:{schema_name=..., connections=...} mapping
    storages = storages,
    -- :get() and :select() provider
    accessor = accessor,
})

local query = [[
    query obtainOrganizationUsers($organization_id: String) {
        relation_st(organization_id: $organization_id, type: "type 1", size: 2) {
            id
            individual_st {
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

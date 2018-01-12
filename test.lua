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
        ],
        "connection": [
            {
                "name": "individual",
                "destination": "individual",
                "parts": [
                    { "source": "individual_id", "destination": "individual_id" }
                ]
            },
            {
                "name": "organization",
                "destination": "organization",
                "parts": [
                    { "source": "organization_id", "destination": "organization_id" }
                ]
            }
        ]
    }
}]])

local function gen_access_function(tname, opts)
    local is_list = opts.is_list
    return function(rootValue, args, info)
        local obj
        if tname == 'relation' then
            obj = {
                id = 'abc',
                type = '123',
                size = 2,
                individual_id = 'def',
                organization_id = 'ghi',
            }
        elseif tname == 'individual' then
            obj = {
                individual_id = 'def',
                last_name = 'last name',
                first_name = 'first name',
                birthdate = '1970-01-01',
            }
        elseif tname == 'organization' then
            obj = {
                organization_id = 'def',
                organization_name = 'qwqw',
            }
        else
            error('NIY: ' .. tname)
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
    -- storage:class_name mapping
    storages = {
		individual = 'individual',
		organization = 'organization',
		relation = 'relation',
	},
    -- :get() and :select() provider
    accessor = accessor,
})

local query = [[
    query obtainOrganizationUsers($organization_id: String) {
        relation(organization_id: $organization_id, type: "type 1", size: 2) {
            id
            individual {
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

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
    "user": {
        "type": "record",
        "name": "user",
        "fields": [
            { "name": "user_str", "type": "string" },
            { "name": "user_num", "type": "long" },
            { "name": "first_name", "type": "string" },
            { "name": "last_name", "type": "string" }
        ]
    }
}]])

local collections = json.decode([[{
    "user_collection": {
        "schema_name": "user",
        "connections": []
    }
}]])

local service_fields = {
    user = {},
}

local indexes = {
    user_collection = {
        user_str_num_index = {
            service_fields = {},
            fields = {'user_str', 'user_num'},
            index_type = 'tree',
            unique = true,
            primary = true,
        },
    },
}

-- fill spaces
-- -----------

-- user_collection fields
local U_USER_STR_FN = 1
local U_USER_NUM_FN = 2

box.cfg{background = false}
box.once('test_space_init_spaces', function()
    box.schema.create_space('user_collection')
    box.space.user_collection:create_index('user_str_num_index',
        {type = 'tree', unique = true, parts = {
            U_USER_STR_FN, 'string', U_USER_NUM_FN, 'unsigned',
        }}
    )
end)

for i = 1, 100 do
    local s = tostring(i)
    box.space.user_collection:replace(
        {'user_str_' .. s, i, 'first name ' .. s, 'last name ' .. s})
end

-- build accessor and graphql schemas
-- ----------------------------------

local accessor = graphql.accessor_space.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
})

local gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
})

-- run queries
-- -----------

local query_1 = [[
    query users($user_str: String, $user_num: Long) {
        user_collection(user_str: $user_str, user_num: $user_num) {
            user_str
            user_num
            last_name
            first_name
        }
    }
]]

utils.show_trace(function()
    local variables_1 = {user_str = 'user_str_42', user_num = 42}
    local gql_query_1 = gql_wrapper:compile(query_1)
    local result = gql_query_1:execute(variables_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))
end)

-- clean up
box.space._schema:delete('oncetest_space_init_spaces')
box.space.user_collection:drop()

os.exit()

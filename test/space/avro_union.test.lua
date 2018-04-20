#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)"):
    gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

box.cfg{ background = false }
local json = require('json')
local yaml = require('yaml')
local graphql = require('graphql')
local utils = require('graphql.utils')
local avro_schema = require('avro_schema')

local schemas = json.decode([[{
    "user_collection": {
        "name": "user_collection",
        "type": "record",
        "fields": [
            { "name": "user_id", "type": "string" },
            { "name": "name", "type": "string" },
            { "name": "stuff", "type": [
                "null",
                "string",
                "int",
                { "type": "map", "values": "int" },
                { "type": "record", "name": "Foo", "fields":[
                    { "name": "foo1", "type": "string" },
                    { "name": "foo2", "type": "string" }
                ]},
                {"type":"array","items": { "type":"map","values":"string" } }
            ]}
        ]
    }
}]])

local collections = json.decode([[{
    "user_collection": {
        "schema_name": "user_collection",
        "connections": []
    }
}]])

local service_fields = {
    user_collection = {
        {name = 'expires_on', type = 'long', default = 0}
    }
}

local indexes = {
    user_collection = {
        user_id_index = {
            service_fields = {},
            fields = {'user_id'},
            index_type = 'tree',
            unique = true,
            primary = true,
        }
    }
}

local USER_ID_FIELD = 2

box.schema.create_space('user_collection')
box.space.user_collection:create_index('user_id_index',
    {type = 'tree', unique = true, parts = { USER_ID_FIELD, 'string' }}
)

local NULL = 0
local STRING = 1
local INT = 2
local MAP = 3
local OBJ = 4
local ARR_MAP = 5

box.space.user_collection:replace(
    {1827767717, 'user_id_0', 'Nobody', NULL, box.NULL})

box.space.user_collection:replace(
    {1827767717, 'user_id_1', 'Zlata', STRING, 'Some string'})

box.space.user_collection:replace(
    {1827767717, 'user_id_2', 'Ivan', INT, 123})

box.space.user_collection:replace(
    {1827767717, 'user_id_3', 'Jane', MAP, {salary = 333, deposit = 444}})

box.space.user_collection:replace(
    {1827767717, 'user_id_4', 'Dan', OBJ, {'foo1 string', 'foo2 string'}})

box.space.user_collection:replace(
    {1827767717, 'user_id_5', 'Max', ARR_MAP,
     {{salary = 'salary string', deposit = 'deposit string'},
     {salary = 'string salary', deposit = 'string deposit'}}})

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

local query_1 = [[
    query user_collection {
        user_collection {
            user_id
            name
            stuff {
                ... on String_box {
                    string
                }

                ... on Int_box {
                    int
                }

                ... on List_box {
                    array
                }

                ... on Map_box {
                    map
                }

                ... on Foo_box {
                    Foo {
                        foo1
                        foo2
                    }
                }
            }
        }
    }
]]

utils.show_trace(function()
    local variables_1 = {}
    local gql_query_1 = gql_wrapper:compile(query_1)
    local result = gql_query_1:execute(variables_1)
    print(('RESULT\n%s'):format(yaml.encode(result)))

    print('Validating results with initial avro-schema')
    local _, schema = avro_schema.create(schemas.user_collection)
    for _, r in ipairs(result.user_collection) do
        local ok, err = avro_schema.validate(schema, r)
        print(ok)
        if not ok then print(err) end
        print(yaml.encode(r))
    end
end)

box.space.user_collection:drop()

os.exit()

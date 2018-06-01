local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local avro = require('avro_schema')
local test_utils = require('test.utils')

local union_testdata = {}

local union_testdata_schemas

function union_testdata.get_test_metadata()
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
    union_testdata_schemas = schemas

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

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function union_testdata.init_spaces()
    local USER_ID_FIELD = 2

    box.once('test_space_init_spaces', function()
        box.schema.create_space('user_collection')
        box.space.user_collection:create_index('user_id_index',
            {type = 'tree', unique = true, parts = { USER_ID_FIELD, 'string' }}
        )
    end)
end

function union_testdata.fill_test_data(shard)
    local shard = shard or box.space

    local NULL = 0
    local STRING = 1
    local INT = 2
    local MAP = 3
    local OBJ = 4
    local ARR_MAP = 5

    shard.user_collection:replace(
        {1827767717, 'user_id_1', 'Nobody', NULL, box.NULL})

    shard.user_collection:replace(
        {1827767717, 'user_id_2', 'Zlata', STRING, 'Some string'})

    shard.user_collection:replace(
        {1827767717, 'user_id_3', 'Ivan', INT, 123})

    shard.user_collection:replace(
        {1827767717, 'user_id_4', 'Jane', MAP, {salary = 333, deposit = 444}})

    shard.user_collection:replace(
        {1827767717, 'user_id_5', 'Dan', OBJ, {'foo1 string', 'foo2 string'}})

    shard.user_collection:replace(
        {1827767717, 'user_id_6', 'Max', ARR_MAP,
         {{salary = 'salary string', deposit = 'deposit string'},
         {salary = 'string salary', deposit = 'string deposit'}}})
end

function union_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
end

function union_testdata.run_queries(gql_wrapper)
    local test = tap.test('union')
    test:plan(7)

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

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local variables_1 = {}

    local result_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1)
    end)

    local exp_result_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          name: Nobody
        - user_id: user_id_2
          name: Zlata
          stuff:
            string: Some string
        - user_id: user_id_3
          name: Ivan
          stuff:
            int: 123
        - user_id: user_id_4
          name: Jane
          stuff:
            map: {'salary': 333, 'deposit': 444}
        - user_id: user_id_5
          name: Dan
          stuff:
            Foo:
              foo1: foo1 string
              foo2: foo2 string
        - user_id: user_id_6
          name: Max
          stuff:
            array:
            - {'salary': 'salary string', 'deposit': 'deposit string'}
            - {'salary': 'string salary', 'deposit': 'string deposit'}
    ]]):strip())

    test:is_deeply(result_1, exp_result_1, '1')

    -- validating results with initial avro-schema
    local schemas = union_testdata_schemas
    local ok, schema = avro.create(schemas.user_collection)
    assert(ok)
    for i, user in ipairs(result_1.user_collection) do
        local ok, res = avro.validate(schema, user)
        test:ok(ok, ('validate %dth user'):format(i), res)
    end

    assert(test:check(), 'check plan')
end

return union_testdata

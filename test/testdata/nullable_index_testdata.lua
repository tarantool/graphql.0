local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

local nullable_index_testdata = {}

-- return an error w/o file name and line number
local function strip_error(err)
    return tostring(err):gsub('^.-:.-: (.*)$', '%1')
end

local function print_and_return(...)
    print(...)
    return table.concat({...}, ' ') .. '\n'
end

function nullable_index_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "foo": {
            "type": "record",
            "name": "foo",
            "fields": [
                { "name": "id", "type": "string" },
                { "name": "bar_id_1", "type": "string" },
                { "name": "bar_id_2", "type": "string" },
                { "name": "data", "type": "string" }
            ]
        },
        "bar": {
            "type": "record",
            "name": "bar",
            "fields": [
                { "name": "id", "type": "string" },
                { "name": "id_or_null_1", "type": "string*" },
                { "name": "id_or_null_2", "type": "string*" },
                { "name": "id_or_null_3", "type": "string*" },
                { "name": "data", "type": "string" }
            ]
        }
    }]])

    local collections = json.decode([[{
        "foo": {
            "schema_name": "foo",
            "connections": [
                {
                    "type": "1:N",
                    "name": "bar_partial_unique",
                    "destination_collection": "bar",
                    "parts": [
                        { "source_field": "bar_id_1", "destination_field": "id_or_null_1" }
                    ],
                    "index_name": "unique_compound_nullable"
                },
                {
                    "type": "1:N",
                    "name": "bar_partial_non_unique",
                    "destination_collection": "bar",
                    "parts": [
                        { "source_field": "bar_id_1", "destination_field": "id_or_null_2" }
                    ],
                    "index_name": "non_unique_compound_nullable"
                }
            ]
        },
        "bar": {
            "schema_name": "bar",
            "connections": []
        }
    }]])

    local service_fields = {
        foo = {},
        bar = {},
    }

    local indexes = {
        foo = {
            pk = {
                service_fields = {},
                fields = {'id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
        bar = {
            pk = {
                service_fields = {},
                fields = {'id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            unique_compound_nullable = {
                service_fields = {},
                fields = {'id_or_null_1', 'id_or_null_2'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
            non_unique_compound_nullable = {
                service_fields = {},
                fields = {'id_or_null_2', 'id_or_null_3'},
                index_type = 'tree',
                unique = false,
                primary = false,
            },
        },
    }

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function nullable_index_testdata.init_spaces()
    -- foo fields
    local FOO_ID_FN = 1

    -- bar fields
    local BAR_ID_FN = 1
    local BAR_ID_OR_NULL_1_FN = 3
    local BAR_ID_OR_NULL_2_FN = 5
    local BAR_ID_OR_NULL_3_FN = 7

    box.once('test_space_init_spaces', function()
        box.schema.create_space('foo')
        box.space.foo:create_index('pk',
            {type = 'tree', unique = true, parts = {
                FOO_ID_FN, 'string',
            }}
        )

        box.schema.create_space('bar')
        box.space.bar:create_index('pk',
            {type = 'tree', unique = true, parts = {
                BAR_ID_FN, 'string',
            }}
        )
        box.space.bar:create_index('unique_compound_nullable',
            {type = 'tree', unique = true, parts = {
                {BAR_ID_OR_NULL_1_FN, 'string', is_nullable = true},
                {BAR_ID_OR_NULL_2_FN, 'string', is_nullable = true},
            }}
        )
        box.space.bar:create_index('non_unique_compound_nullable',
            {type = 'tree', unique = false, parts = {
                {BAR_ID_OR_NULL_2_FN, 'string', is_nullable = true},
                {BAR_ID_OR_NULL_3_FN, 'string', is_nullable = true},
            }}
        )
    end)
end

function nullable_index_testdata.fill_test_data(shard)
    local shard = shard or box.space

    local NULL_T = 0
    local STRING_T = 1

    for i = 1, 100 do
        local s = tostring(i)
        shard.foo:replace({s, s, s, s})
    end
    -- str, str, str
    for i = 1, 100 do
        local s = tostring(i)
        shard.bar:replace({s, STRING_T, s, STRING_T, s, STRING_T, s, s})
    end
    -- null, str, str
    local s = '101'
    shard.bar:replace({s, NULL_T, box.NULL, STRING_T, s, STRING_T, s, s})
    local s = '102'
    shard.bar:replace({s, NULL_T, box.NULL, STRING_T, s, STRING_T, s, s})
    -- str, null, str
    local s = '103'
    shard.bar:replace({s, STRING_T, s, NULL_T, box.NULL, STRING_T, s, s})
    local s = '104'
    shard.bar:replace({s, STRING_T, s, NULL_T, box.NULL, STRING_T, s, s})
    -- str, str, null
    local s = '105'
    shard.bar:replace({s, STRING_T, s, STRING_T, s, NULL_T, box.NULL, s})
    local s = '106'
    shard.bar:replace({s, STRING_T, s, STRING_T, s, NULL_T, box.NULL, s})
    -- null, null, str
    local s = '107'
    shard.bar:replace(
        {s, NULL_T, box.NULL, NULL_T, box.NULL, STRING_T, s, s})
    local s = '108'
    shard.bar:replace(
        {s, NULL_T, box.NULL, NULL_T, box.NULL, STRING_T, s, s})
    -- null, str, null
    local s = '109'
    shard.bar:replace(
        {s, NULL_T, box.NULL, STRING_T, s, NULL_T, box.NULL, s})
    local s = '110'
    shard.bar:replace(
        {s, NULL_T, box.NULL, STRING_T, s, NULL_T, box.NULL, s})
    -- str, null, null
    local s = '111'
    shard.bar:replace(
        {s, STRING_T, s, NULL_T, box.NULL, NULL_T, box.NULL, s})
    local s = '112'
    shard.bar:replace(
        {s, STRING_T, s, NULL_T, box.NULL, NULL_T, box.NULL, s})
    -- null, null, null
    local s = '113'
    shard.bar:replace(
        {s, NULL_T, box.NULL, NULL_T, box.NULL, NULL_T, box.NULL, s})
    local s = '114'
    shard.bar:replace(
        {s, NULL_T, box.NULL, NULL_T, box.NULL, NULL_T, box.NULL, s})
end

function nullable_index_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.foo:drop()
    box.space.bar:drop()
end

function nullable_index_testdata.run_queries(gql_wrapper)
    local results = ''

    -- verify that null as an argument value is forbidden
    -- --------------------------------------------------

    local query_1 = [[
        query get_bar {
            bar(id_or_null_1: null) {
                id
                id_or_null_1
            }
        }
    ]]

    local ok, err = pcall(function()
        local gql_query_1 = gql_wrapper:compile(query_1)
        local variables_1 = {}
        local result = gql_query_1:execute(variables_1)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    results = results .. print_and_return(('RESULT: ok: %s; err: %s'):format(tostring(ok), strip_error(err)))

    -- top-level objects: full secondary index (unique / non-unique)
    -- -------------------------------------------------------------

    local query_2 = [[
        query get_bar($id_or_null_1: String, $id_or_null_2: String,
                $id_or_null_3: String) {
            bar(id_or_null_1: $id_or_null_1, id_or_null_2: $id_or_null_2,
                    id_or_null_3: $id_or_null_3, limit: 20) {
                id
                id_or_null_1
                id_or_null_2
                id_or_null_3
            }
        }
    ]]

    local gql_query_2 = gql_wrapper:compile(query_2)

    -- fullscan; expected to see objects with ID > 100
    utils.show_trace(function()
        local variables_2_1 = {}
        local result = gql_query_2:execute(variables_2_1)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    -- lookup by the unique index; expected to see only the object with ID 42
    utils.show_trace(function()
        local variables_2_2 = {
            id_or_null_1 = '42',
            id_or_null_2 = '42',
        }
        local result = gql_query_2:execute(variables_2_2)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    -- lookup by the non-unique index; expected to see only the object with ID
    -- 42
    utils.show_trace(function()
        local variables_2_3 = {
            id_or_null_2 = '42',
            id_or_null_3 = '42',
        }
        local result = gql_query_2:execute(variables_2_3)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    -- connection: partial match with compound secondary index (nullable part
    -- within a key / outside, but within the index)
    -- ----------------------------------------------------------------------

    local query_3 = [[
        query get_foo($id: String) {
            foo(id: $id) {
                id
                bar_partial_unique {
                    id
                }
                bar_partial_non_unique {
                    id
                }
            }
        }
    ]]

    utils.show_trace(function()
        local variables_3 = {id = '42'}
        local gql_query_3 = gql_wrapper:compile(query_3)
        local result = gql_query_3:execute(variables_3)
        results = results .. print_and_return(
            ('RESULT\n%s'):format(yaml.encode(result)))
    end)

    return results
end

return nullable_index_testdata

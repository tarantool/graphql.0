-- ----------------------------------------------------------
-- Motivation: https://github.com/tarantool/graphql/issues/43
-- and https://github.com/tarantool/shard/issues/61
-- ----------------------------------------------------------

local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.utils')

local nullable_index_testdata = {}

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

function nullable_index_testdata.init_spaces(avro_version)
    -- foo fields
    local FOO_ID_FN = 1

    -- bar fields
    local BAR_ID_FN = 1
    local BAR_ID_OR_NULL_1_FN = avro_version == 3 and 2 or 3
    local BAR_ID_OR_NULL_2_FN = avro_version == 3 and 3 or 5
    local BAR_ID_OR_NULL_3_FN = avro_version == 3 and 4 or 7

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

function nullable_index_testdata.fill_test_data(virtbox, meta)
    for i = 1, 114 do
        local s = tostring(i)
        test_utils.replace_object(virtbox, meta, 'foo', {
            id = s,
            bar_id_1 = s,
            bar_id_2 = s,
            data = s,
        })
    end
    -- str, str, str
    for i = 1, 100 do
        local s = tostring(i)
        test_utils.replace_object(virtbox, meta, 'bar', {
            id = s,
            id_or_null_1 = s,
            id_or_null_2 = s,
            id_or_null_3 = s,
            data = s,
        })
    end
    -- null, str, str
    local s = '101'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = s,
        id_or_null_3 = s,
        data = s,
    })
    local s = '102'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = s,
        id_or_null_3 = s,
        data = s,
    })
    -- str, null, str
    local s = '103'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = s,
        id_or_null_2 = box.NULL,
        id_or_null_3 = s,
        data = s,
    })
    local s = '104'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = s,
        id_or_null_2 = box.NULL,
        id_or_null_3 = s,
        data = s,
    })
    -- str, str, null
    local s = '105'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = s,
        id_or_null_2 = s,
        id_or_null_3 = box.NULL,
        data = s,
    })
    local s = '106'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = s,
        id_or_null_2 = s,
        id_or_null_3 = box.NULL,
        data = s,
    })
    -- null, null, str
    local s = '107'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = box.NULL,
        id_or_null_3 = s,
        data = s,
    })
    local s = '108'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = box.NULL,
        id_or_null_3 = s,
        data = s,
    })
    -- null, str, null
    local s = '109'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = s,
        id_or_null_3 = box.NULL,
        data = s,
    })
    local s = '110'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = s,
        id_or_null_3 = box.NULL,
        data = s,
    })
    -- str, null, null
    local s = '111'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = s,
        id_or_null_2 = box.NULL,
        id_or_null_3 = box.NULL,
        data = s,
    })
    local s = '112'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = s,
        id_or_null_2 = box.NULL,
        id_or_null_3 = box.NULL,
        data = s,
    })
    -- null, null, null
    local s = '113'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = box.NULL,
        id_or_null_3 = box.NULL,
        data = s,
    })
    local s = '114'
    test_utils.replace_object(virtbox, meta, 'bar', {
        id = s,
        id_or_null_1 = box.NULL,
        id_or_null_2 = box.NULL,
        id_or_null_3 = box.NULL,
        data = s,
    })
end

function nullable_index_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.foo:drop()
    box.space.bar:drop()
end

function nullable_index_testdata.run_queries(gql_wrapper)
    local test = tap.test('nullable_index')
    test:plan(6)

    -- {{{ verify that null as an argument value is forbidden

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
        return gql_query_1:execute(variables_1)
    end)

    local result = {ok = ok, err = test_utils.strip_error(err)}
    local exp_result = yaml.decode(([[
        ---
        ok: false
        err: Syntax error near line 2
    ]]):strip())
    test:is_deeply(result, exp_result, '1')

    -- }}}
    -- {{{ top-level objects: full secondary index (unique / non-unique)

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

    local gql_query_2 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    -- fullscan; expected to see objects with ID > 100
    local result = test_utils.show_trace(function()
        local variables_2_1 = {}
        return gql_query_2:execute(variables_2_1)
    end)
    local exp_result = yaml.decode(([[
        ---
        bar:
        - id_or_null_1: '1'
          id_or_null_3: '1'
          id_or_null_2: '1'
          id: '1'
        - id_or_null_1: '10'
          id_or_null_3: '10'
          id_or_null_2: '10'
          id: '10'
        - id_or_null_1: '100'
          id_or_null_3: '100'
          id_or_null_2: '100'
          id: '100'
        - id_or_null_3: '101'
          id_or_null_2: '101'
          id: '101'
        - id_or_null_3: '102'
          id_or_null_2: '102'
          id: '102'
        - id_or_null_1: '103'
          id_or_null_3: '103'
          id: '103'
        - id_or_null_1: '104'
          id_or_null_3: '104'
          id: '104'
        - id_or_null_1: '105'
          id_or_null_2: '105'
          id: '105'
        - id_or_null_1: '106'
          id_or_null_2: '106'
          id: '106'
        - id_or_null_3: '107'
          id: '107'
        - id_or_null_3: '108'
          id: '108'
        - id_or_null_2: '109'
          id: '109'
        - id_or_null_1: '11'
          id_or_null_3: '11'
          id_or_null_2: '11'
          id: '11'
        - id_or_null_2: '110'
          id: '110'
        - id_or_null_1: '111'
          id: '111'
        - id_or_null_1: '112'
          id: '112'
        - id: '113'
        - id: '114'
        - id_or_null_1: '12'
          id_or_null_3: '12'
          id_or_null_2: '12'
          id: '12'
        - id_or_null_1: '13'
          id_or_null_3: '13'
          id_or_null_2: '13'
          id: '13'
    ]]):strip())
    test:is_deeply(result, exp_result, '2_1')

    -- lookup by the unique index; expected to see only the object with ID 42
    local result = test_utils.show_trace(function()
        local variables_2_2 = {
            id_or_null_1 = '42',
            id_or_null_2 = '42',
        }
        return gql_query_2:execute(variables_2_2)
    end)
    local exp_result = yaml.decode(([[
        ---
        bar:
        - id_or_null_1: '42'
          id_or_null_3: '42'
          id_or_null_2: '42'
          id: '42'
    ]]):strip())
    test:is_deeply(result, exp_result, '2_2')

    -- lookup by the non-unique index; expected to see only the object with ID
    -- 42
    local result = test_utils.show_trace(function()
        local variables_2_3 = {
            id_or_null_2 = '42',
            id_or_null_3 = '42',
        }
        return gql_query_2:execute(variables_2_3)
    end)
    local exp_result = yaml.decode(([[
        ---
        bar:
        - id_or_null_1: '42'
          id_or_null_3: '42'
          id_or_null_2: '42'
          id: '42'
    ]]):strip())
    test:is_deeply(result, exp_result, '2_3')

    -- }}}
    -- {{{ connection: partial match with compound secondary index (nullable
    -- part within a key / outside, but within the index)

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

    local gql_query_3 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_3)
    end)

    local variables_3_1 = {id = '42'}
    local result_3_1 = test_utils.show_trace(function()
        return gql_query_3:execute(variables_3_1)
    end)
    local exp_result_3_1 = yaml.decode(([[
        ---
        foo:
        - bar_partial_unique:
          - id: '42'
          bar_partial_non_unique:
          - id: '42'
          id: '42'
    ]]):strip())
    test:is_deeply(result_3_1, exp_result_3_1, '3_1')

    local variables_3_2 = {id = '103'}
    local result_3_2 = test_utils.show_trace(function()
        return gql_query_3:execute(variables_3_2)
    end)
    local exp_result_3_2 = yaml.decode(([[
        ---
        foo:
        - bar_partial_unique:
          - id: '103'
          bar_partial_non_unique: []
          id: '103'
    ]]):strip())
    test:is_deeply(result_3_2, exp_result_3_2, '3_2')

    -- }}}

    assert(test:check(), 'check plan')
end

return nullable_index_testdata

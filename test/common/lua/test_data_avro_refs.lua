-- Avro-schema references
-- https://github.com/tarantool/graphql/issues/116

-- XXX: check 'fixed' type when we'll support it
-- XXX: check 'enum' type when we'll support it

local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

local testdata = {}

testdata.meta = {
    schemas = json.decode([[{
        "foo": {
            "type": "record",
            "name": "foo",
            "fields": [
                {"name": "id", "type": "long"},
                {
                    "name": "bar",
                    "type": {
                        "type": "record",
                        "name": "bar",
                        "fields": [
                            {"name": "x", "type": "long"},
                            {"name": "y", "type": "long"}
                        ]
                    }
                },
                {
                    "name": "bar_ref",
                    "type": "bar"
                },
                {
                    "name": "bar_nref",
                    "type": "bar*"
                },
                {
                    "name": "baz",
                    "type": {
                        "type": "record*",
                        "name": "baz",
                        "fields": [
                            {"name": "x", "type": "long"},
                            {"name": "y", "type": "long"}
                        ]
                    }
                },
                {
                    "name": "baz_ref",
                    "type": "baz"
                },
                {
                    "name": "baz_nref",
                    "type": "baz*"
                }
            ]
        }
    }]]),
    collections = json.decode([[{
        "foo": {
            "schema_name": "foo",
            "connections": []
        }
    }]]),
    service_fields = {
        foo = {},
    },
    indexes = {
        foo = {
            id = {
                service_fields = {},
                fields = {'id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
    }
}

function testdata.init_spaces()
    -- foo fields
    local ID_FN = 1

    box.schema.create_space('foo')
    box.space.foo:create_index('id', {
        type = 'tree', unique = true, parts = {ID_FN, 'unsigned'}})
end

function testdata.drop_spaces()
    box.space.foo:drop()
end

function testdata.fill_test_data(virtbox)
    local NULL_T = 0
    local VALUE_T = 1

    local x = 1000
    local y = 2000
    local a = 3000
    local b = 4000

    -- non-null bar, baz and its refs
    virtbox.foo:replace({1,                       -- id
        x, y, x, y, VALUE_T, {x, y},              -- bar & refs
        VALUE_T, {a, b}, a, b, VALUE_T, {a, b},   -- baz & refs
    })
    -- null in nullable bar, baz refs
    virtbox.foo:replace({2,                       -- id
        x, y, x, y, NULL_T, box.NULL,             -- bar & refs
        NULL_T, box.NULL, a, b, NULL_T, box.NULL, -- baz & refs
    })
end

function testdata.run_queries(gql_wrapper)
    local test = tap.test('avro_refs')
    test:plan(2)

    local query_1 = [[
        query get_by_id($id: Long) {
            foo(id: $id) {
                id
                bar {
                    x
                    y
                }
                bar_ref {
                    x
                    y
                }
                bar_nref {
                    x
                    y
                }
                baz {
                    x
                    y
                }
                baz_ref {
                    x
                    y
                }
                baz_nref {
                    x
                    y
                }
            }
        }
    ]]

    local gql_query_1 = utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local variables_1_1 = {id = 1}
    local result_1_1 = utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)

    local exp_result_1_1 = yaml.decode(([[
        ---
        foo:
        - id: 1
          bar:
            x: 1000
            y: 2000
          bar_ref:
            x: 1000
            y: 2000
          bar_nref:
            x: 1000
            y: 2000
          baz:
            x: 3000
            y: 4000
          baz_ref:
            x: 3000
            y: 4000
          baz_nref:
            x: 3000
            y: 4000
    ]]):strip())

    test:is_deeply(result_1_1, exp_result_1_1, '1_1')

    local variables_1_2 = {id = 2}
    local result_1_2 = utils.show_trace(function()
        return gql_query_1:execute(variables_1_2)
    end)

    local exp_result_1_2 = yaml.decode(([[
        ---
        foo:
        - id: 2
          bar:
            x: 1000
            y: 2000
          bar_ref:
            x: 1000
            y: 2000
          baz_ref:
            x: 3000
            y: 4000
    ]]):strip())

    test:is_deeply(result_1_2, exp_result_1_2, '1_2')

    assert(test:check(), 'check plan')
end

return testdata

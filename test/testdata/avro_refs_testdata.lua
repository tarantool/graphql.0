-- Avro-schema references
-- https://github.com/tarantool/graphql/issues/116

-- XXX: check 'fixed' type when we'll support it
-- XXX: check 'enum' type when we'll support it

local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local testdata = {}

function testdata.get_test_metadata()
    local schemas = json.decode([[{
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
                {"name": "bar_ref_array", "type": {
                    "type": "array",
                    "items": "bar"
                }},
                {"name": "bar_nref_array", "type": {
                    "type": "array",
                    "items": "bar*"
                }},
                {"name": "bar_ref_map", "type": {
                    "type": "map",
                    "values": "bar"
                }},
                {"name": "bar_nref_map", "type": {
                    "type": "map",
                    "values": "bar*"
                }},
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
    }]])

    return {
        schemas = schemas,
        -- added foo_2: check compiling metainfo with two usages of the same
        -- schema
        collections = json.decode([[{
            "foo": {
                "schema_name": "foo",
                "connections": []
            },
            "foo_2": {
                "schema_name": "foo",
                "connections": []
            }
        }]]),
        service_fields = {
            foo = {},
            foo_2 = {},
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
            foo_2 = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
            },
        },
    }
end

function testdata.init_spaces()
    -- foo fields
    local ID_FN = 1

    box.schema.create_space('foo')
    box.space.foo:create_index('id', {
        type = 'tree', unique = true, parts = {ID_FN, 'unsigned'}})

    box.schema.create_space('foo_2')
    box.space.foo_2:create_index('id', {
        type = 'tree', unique = true, parts = {ID_FN, 'unsigned'}})
end

function testdata.drop_spaces()
    box.space.foo:drop()
    box.space.foo_2:drop()
end

function testdata.fill_test_data(virtbox, meta)
    local x = 1000
    local y = 2000
    local a = 3000
    local b = 4000

    -- non-null bar, baz and its refs
    local obj_1 = {
        id = 1,
        bar = {x = x, y = y},
        bar_ref = {x = x, y = y},
        bar_nref = {x = x, y = y},
        bar_ref_array = {{x = x, y = y}},
        bar_nref_array = {{x = x, y = y}},
        bar_ref_map = {xy = {x = x, y = y}},
        bar_nref_map = {xy = {x = x, y = y}},
        baz = {x = a, y = b},
        baz_ref = {x = a, y = b},
        baz_nref = {x = a, y = b},
    }
    -- null in nullable bar, baz refs
    local obj_2 = {
        id = 2,
        bar = {x = x, y = y},
        bar_ref = {x = x, y = y},
        bar_nref = box.NULL,
        bar_ref_array = {{x = x, y = y}},
        bar_nref_array = {},
        bar_ref_map = {xy = {x = x, y = y}},
        bar_nref_map = {xy = box.NULL},
        baz = box.NULL,
        baz_ref = {x = a, y = b},
        baz_nref = box.NULL,
    }

    -- replaces
    test_utils.replace_object(virtbox, meta, 'foo', obj_1)
    test_utils.replace_object(virtbox, meta, 'foo', obj_2)
    test_utils.replace_object(virtbox, meta, 'foo_2', obj_1)
    test_utils.replace_object(virtbox, meta, 'foo_2', obj_2)
end

function testdata.run_queries(gql_wrapper)
    local test = tap.test('avro_refs')
    test:plan(4)

    local query_1 = [[
        query get_by_id($id: Long) {
            foo(id: $id) {
                id
                bar            {x, y}
                bar_ref        {x, y}
                bar_nref       {x, y}
                bar_ref_array  {x, y}
                bar_nref_array {x, y}
                bar_ref_map
                bar_nref_map
                baz            {x, y}
                baz_ref        {x, y}
                baz_nref       {x, y}
            }
        }
    ]]
    local query_1_p = query_1:gsub('foo', 'foo_2')

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)
    local gql_query_1_p = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1_p)
    end)

    local variables_1_1 = {id = 1}
    local result_1_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)
    local result_1_1_p = test_utils.show_trace(function()
        return gql_query_1_p:execute(variables_1_1)
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
          bar_ref_array:
          - x: 1000
            y: 2000
          bar_nref_array:
          - x: 1000
            y: 2000
          bar_ref_map:
            xy:
              x: 1000
              y: 2000
          bar_nref_map:
            xy:
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
    local exp_result_1_1_p = {foo_2 = exp_result_1_1.foo}

    test:is_deeply(result_1_1.data, exp_result_1_1, '1_1')
    test:is_deeply(result_1_1_p.data, exp_result_1_1_p, '1_1_p')

    local variables_1_2 = {id = 2}
    local result_1_2 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_2)
    end)
    local result_1_2_p = test_utils.show_trace(function()
        return gql_query_1_p:execute(variables_1_2)
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
          bar_ref_array:
          - x: 1000
            y: 2000
          bar_nref_array: []
          bar_ref_map:
            xy:
              x: 1000
              y: 2000
          bar_nref_map: {}
          baz_ref:
            x: 3000
            y: 4000
    ]]):strip())
    local exp_result_1_2_p = {foo_2 = exp_result_1_2.foo}

    test:is_deeply(result_1_2.data, exp_result_1_2, '1_2')
    test:is_deeply(result_1_2_p.data, exp_result_1_2_p, '1_2_p')

    assert(test:check(), 'check plan')
end

return testdata

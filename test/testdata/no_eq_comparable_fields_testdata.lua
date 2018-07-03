-- https://github.com/tarantool/graphql/issues/183
--
-- The problem is that an InputObject type can be generated with empty fields.

local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local no_eq_comparable_fields_testdata = {}

function no_eq_comparable_fields_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "no_eq_comparable_fields": {
            "type": "record",
            "name": "no_eq_comparable_fields",
            "fields": [
                { "name": "id", "type": "float"},
                { "name": "float_value", "type": "float" },
                { "name": "double_value", "type": "double" }
            ]
        }
    }]])

    local collections = json.decode([[{
        "no_eq_comparable_fields": {
            "schema_name": "no_eq_comparable_fields",
            "connections": []
        }
    }]])

    local service_fields = {
        no_eq_comparable_fields = {},
    }

    local indexes = {
        no_eq_comparable_fields = {
            primary = {
                service_fields = {},
                fields = {'id'},
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

function no_eq_comparable_fields_testdata.init_spaces()
    -- no_eq_comparable_fields fields
    local N_ID_FN = 1

    box.once('test_space_init_spaces', function()
        box.schema.create_space('no_eq_comparable_fields')
        box.space.no_eq_comparable_fields:create_index(
            'primary',
            {type = 'tree', parts = {
                N_ID_FN, 'number'
            }}
        )
    end)
end

function no_eq_comparable_fields_testdata.fill_test_data(virtbox, meta)
    test_utils.replace_object(virtbox, meta, 'no_eq_comparable_fields', {
        id = 1.0,
        float_value = 1.0,
        double_value = 1.0,
    })
    test_utils.replace_object(virtbox, meta, 'no_eq_comparable_fields', {
        id = 2.0,
        float_value = 2.0,
        double_value = 2.0,
    })
end

function no_eq_comparable_fields_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.no_eq_comparable_fields:drop()
end

function no_eq_comparable_fields_testdata.run_queries(gql_wrapper)
    local test = tap.test('no_eq_comparable_fields')
    test:plan(2)

    local query_1 = [[
        {
            no_eq_comparable_fields(limit: 1) {
                id
                float_value
                double_value
            }
        }
    ]]

    local exp_result_1 = yaml.decode(([[
        ---
        no_eq_comparable_fields:
        - id: 1.0
          float_value: 1.0
          double_value: 1.0
    ]]):strip())

    test_utils.show_trace(function()
        local gql_query_1 = gql_wrapper:compile(query_1)
        local result = gql_query_1:execute({})
        local exp_result_1 = test_utils.deeply_number_tostring(exp_result_1)
        local result = test_utils.deeply_number_tostring(result)
        test:is_deeply(result.data, exp_result_1, 'limit works')
    end)

    local introspection_query = [[
        query IntrospectionQuery {
            __schema {
                types {
                    kind
                    name
                    fields {
                        name
                    }
                    inputFields {
                        name
                    }
                }
            }
        }
    ]]

    local result = test_utils.show_trace(function()
        local gql_introspection_query = gql_wrapper:compile(introspection_query)
        return gql_introspection_query:execute({})
    end)

    local ok = true
    for _, t in ipairs(result.data.__schema.types) do
        if t.kind == 'OBJECT' or t.kind == 'INPUT_OBJECT' then
            local fields = t.kind == 'OBJECT' and t.fields or t.inputFields
            ok = ok and type(fields) == 'table' and #fields > 0
            assert(ok, ('wrong %s: %s'):format(t.kind, t.name))
        end
    end

    test:ok(ok, 'no Object/InputObject types with no fields')

    assert(test:check(), 'check plan')
end

return no_eq_comparable_fields_testdata

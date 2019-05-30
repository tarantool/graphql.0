local json = require('json')
local test_utils = require('test.test_utils')

local scalar_testdata = {}

function scalar_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "foo": {
            "type": "record",
            "name": "foo",
            "fields": [
                { "name": "id", "type": "long" },
                { "name": "boolean_field", "type": "boolean" },
                { "name": "int_field", "type": "int" },
                { "name": "long_field", "type": "long" },
                { "name": "float_field", "type": "float" },
                { "name": "double_field", "type": "double" },
                { "name": "string_field", "type": "string" }
            ]
        }
    }]])

    local collections = json.decode([[{
        "foo": {
            "schema_name": "foo",
            "connections": []
        }
    }]])

    local service_fields = {
        foo = {}
    }

    local indexes = {
        foo = {
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

function scalar_testdata.init_spaces()
    local FOO_ID_FN = 1

    box.once('test_space_init_spaces', function()
        box.schema.create_space('foo')
        box.space.foo:create_index('primary',
            {type = 'tree', unique = true, parts = {
                FOO_ID_FN, 'unsigned'
            }}
        )
    end)
end

function scalar_testdata.fill_test_data(virtbox, meta)
    test_utils.replace_object(virtbox, meta, 'foo', {
        id = 1,
        boolean_field = true,
        int_field = 1,
        long_field = 1,
        float_field = 1.0,
        double_field = 1.0,
        string_field = 'one',
    })
end

function scalar_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.foo:drop()
end

return scalar_testdata

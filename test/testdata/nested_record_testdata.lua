-- Nested record inside a record
-- https://github.com/tarantool/graphql/issues/46
-- https://github.com/tarantool/graphql/issues/49

local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.utils')

local testdata = {}

testdata.meta = {
    schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                {"name": "uid", "type": "long"},
                {"name": "p1", "type": "string"},
                {"name": "p2", "type": "string"},
                {
                    "name": "nested",
                    "type": {
                        "type": "record",
                        "name": "nested",
                        "fields": [
                            {"name": "x", "type": "long"},
                            {"name": "y", "type": "long"}
                        ]
                    }
                }
            ]
        }
    }]]),
    collections = json.decode([[{
        "user": {
            "schema_name": "user",
            "connections": []
        }
    }]]),
    service_fields = {
        user = {},
    },
    indexes = {
        user = {
            uid = {
                service_fields = {},
                fields = {'uid'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
    }
}


function testdata.init_spaces()
    -- user fields
    local UID_FN = 1

    box.schema.create_space('user')
    box.space.user:create_index('uid', {
        type = 'tree', unique = true, parts = {UID_FN, 'unsigned'}})
end

function testdata.drop_spaces()
    box.space.user:drop()
end

function testdata.fill_test_data(virtbox, meta)
    for i = 1, 15 do
        local uid = i
        local p1 = 'p1 ' .. tostring(i)
        local p2 = 'p2 ' .. tostring(i)
        local x = 1000 + i
        local y = 2000 + i
        test_utils.replace_object(virtbox, meta, 'user', {
            uid = uid,
            p1 = p1,
            p2 = p2,
            nested = {
                x = x,
                y = y,
            }
        })
    end
end

function testdata.run_queries(gql_wrapper)
    local test = tap.test('nested_record')
    test:plan(1)

    local query_1 = [[
        query getUserByUid($uid: Long) {
            user(uid: $uid) {
                uid
                p1
                p2
                nested {
                    x
                    y
                }
            }
        }
    ]]

    local variables_1 = {uid = 5}
    local result_1 = test_utils.show_trace(function()
        local gql_query_1 = gql_wrapper:compile(query_1)
        return gql_query_1:execute(variables_1)
    end)

    local exp_result_1 = yaml.decode(([[
        ---
        user:
        - uid: 5
          p1: p1 5
          p2: p2 5
          nested:
            x: 1005
            y: 2005
    ]]):strip())

    test:is_deeply(result_1, exp_result_1, '1')

    -- XXX: uncomment when arguments for nested records will be supported
    --[=[
    local query_2 = [[
        query getUserByX($x: Long) {
            user(nested: {x: $x}) {
                uid
                p1
                p2
                nested {
                    x
                    y
                }
            }
        }
    ]]

    local variables_2 = {x = 1005}
    local result_2 = test_utils.show_trace(function()
        local gql_query_2 = gql_wrapper:compile(query_2)
        return gql_query_2:execute(variables_2)
    end)

    local exp_result_2 = yaml.decode(([[
        ---
        user:
        - uid: 5
          p1: p1 5
          p2: p2 5
          nested:
            x: 1005
            y: 2005
    ]]):strip())

    test:is_deeply(result_2, exp_result_2, '2')
    ]=]--

    assert(test:check(), 'check plan')
end

return testdata

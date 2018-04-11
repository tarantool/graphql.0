-- Nested record inside a record
-- https://github.com/tarantool/graphql/issues/46
-- https://github.com/tarantool/graphql/issues/49

local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

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

function testdata.fill_test_data(virtbox)
    for i = 1, 15 do
        local uid = i
        local p1 = 'p1 ' .. tostring(i)
        local p2 = 'p2 ' .. tostring(i)
        local x = 1000 + i
        local y = 2000 + i
        virtbox.user:replace({uid, p1, p2, x, y})
    end
end

function testdata.run_queries(gql_wrapper)
    local output = ''

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
    local result_1 = utils.show_trace(function()
        local gql_query_1 = gql_wrapper:compile(query_1)
        return gql_query_1:execute(variables_1)
    end)

    output = output .. 'RUN 1 {{{\n' ..
        (('QUERY\n%s'):format(query_1:rstrip())) .. '\n' ..
        (('VARIABLES\n%s'):format(yaml.encode(variables_1))) .. '\n' ..
        (('RESULT\n%s'):format(yaml.encode(result_1))) .. '\n' ..
        '}}}\n'

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
    local result_2 = utils.show_trace(function()
        local gql_query_2 = gql_wrapper:compile(query_2)
        return gql_query_2:execute(variables_2)
    end)

    output = output .. 'RUN 2 {{{\n' ..
        (('QUERY\n%s'):format(query_2:rstrip())) .. '\n' ..
        (('VARIABLES\n%s'):format(yaml.encode(variables_2))) .. '\n' ..
        (('RESULT\n%s'):format(yaml.encode(result_2))) .. '\n' ..
        '}}}\n'
    ]=]--

    return output:rstrip()
end

return testdata

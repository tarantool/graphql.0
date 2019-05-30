#!/usr/bin/env tarantool

local fio = require('fio')
local yaml = require('yaml')
local avro = require('avro_schema')
local tap = require('tap')

-- require in-repo version of graphql/ sources despite current working directory
local cur_dir = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', ''))
package.path =
    cur_dir .. '/../../?/init.lua' .. ';' ..
    cur_dir .. '/../../?.lua' .. ';' ..
    package.path

local utils = require('graphql.utils')
local graphql = require('graphql')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.scalar_testdata')

local cases = {
    {
        'verify boolean',
        query = [[
            {
                foo(id: 1) {
                    boolean_field
                }
            }
        ]],
        exp_avro_schema = [[
            ---
            type: record
            name: Query
            fields:
            - name: foo
              type:
                type: array
                items:
                  type: record
                  name: foo
                  namespace: Query
                  fields:
                  - name: boolean_field
                    type: boolean
        ]],
        exp_result = [[
            ---
            foo:
            - boolean_field: true
        ]],
    },
    {
        'verify int',
        query = [[
            {
                foo(id: 1) {
                    int_field
                }
            }
        ]],
        exp_avro_schema = [[
            ---
            type: record
            name: Query
            fields:
            - name: foo
              type:
                type: array
                items:
                  type: record
                  name: foo
                  namespace: Query
                  fields:
                  - name: int_field
                    type: int
        ]],
        exp_result = [[
            ---
            foo:
            - int_field: 1
        ]],
    },
    {
        'verify long',
        query = [[
            {
                foo(id: 1) {
                    long_field
                }
            }
        ]],
        exp_avro_schema = [[
            ---
            type: record
            name: Query
            fields:
            - name: foo
              type:
                type: array
                items:
                  type: record
                  name: foo
                  namespace: Query
                  fields:
                  - name: long_field
                    type: long
        ]],
        exp_result = [[
            ---
            foo:
            - long_field: 1
        ]],
    },
    {
        'verify float',
        query = [[
            {
                foo(id: 1) {
                    float_field
                }
            }
        ]],
        -- Note: GraphQL Float is double precision, so we convert is back as
        -- avro-schema double.
        -- http://graphql.org/learn/schema/#scalar-types
        exp_avro_schema = [[
            ---
            type: record
            name: Query
            fields:
            - name: foo
              type:
                type: array
                items:
                  type: record
                  name: foo
                  namespace: Query
                  fields:
                  - name: float_field
                    type: double
        ]],
        exp_result = [[
            ---
            foo:
            - float_field: 1.0
        ]],
    },
    {
        'verify double',
        query = [[
            {
                foo(id: 1) {
                    double_field
                }
            }
        ]],
        exp_avro_schema = [[
            ---
            type: record
            name: Query
            fields:
            - name: foo
              type:
                type: array
                items:
                  type: record
                  name: foo
                  namespace: Query
                  fields:
                  - name: double_field
                    type: double
        ]],
        exp_result = [[
            ---
            foo:
            - double_field: 1.0
        ]],
    },
    {
        'verify string',
        query = [[
            {
                foo(id: 1) {
                    string_field
                }
            }
        ]],
        exp_avro_schema = [[
            ---
            type: record
            name: Query
            fields:
            - name: foo
              type:
                type: array
                items:
                  type: record
                  name: foo
                  namespace: Query
                  fields:
                  - name: string_field
                    type: string
        ]],
        exp_result = [[
            ---
            foo:
            - string_field: one
        ]],
    },
}

local test = tap.test('convert graphql scalars to avro-schema')
test:plan(#cases)

-- Prepare data and create a graphql instance.
local meta = testdata.get_test_metadata()
box.cfg{}
testdata.init_spaces()
testdata.fill_test_data(box.space, meta)
local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = meta.schemas,
    collections = meta.collections,
    indexes = meta.indexes,
    service_fields = meta.service_fields,
    accessor = 'space',
}, test_utils.test_conf_graphql_opts()))

-- Run cases.
for _, case in ipairs(cases) do
    test:test(case[1], function(test)
        test:plan(4)
        local gql_query = gql_wrapper:compile(case.query)

        local avro_schema = gql_query:avro_schema()
        local exp_avro_schema = yaml.decode(case.exp_avro_schema:strip())
        test:is_deeply(avro_schema, exp_avro_schema,
            'verify generated avro schema')

        local result = gql_query:execute(case.variables or {})
        local exp_result = yaml.decode(case.exp_result:strip())
        test:is_deeply(result.data, exp_result, 'verify query result')

        local ok, handle = avro.create(avro_schema)
        assert(ok, 'create avro-schema handle')
        local ok = avro.validate(handle, result.data)
        test:ok(ok, 'verify query result against avro-schema')

        local ok, model = avro.compile(handle)
        assert(ok, 'compile avro-schema')
        local ok, tuple = model.flatten(result.data)
        assert(ok, 'flatten query result')
        local ok, object = model.unflatten(tuple)
        assert(ok, 'unflatten flattened query result')
        test:is_deeply(object, exp_result,
            'verify flatten-unflatten gives the same result')
    end)
end

testdata.drop_spaces()

os.exit(test:check() == true and 0 or 1)

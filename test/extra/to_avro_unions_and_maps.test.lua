#!/usr/bin/env tarantool

local fio = require('fio')
local yaml = require('yaml')
local avro = require('avro_schema')
local tap = require('tap')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local graphql = require('graphql')
local utils = require('graphql.utils')
local test_utils = require('test.test_utils')
local vb = require('test.virtual_box')
local testdata = require('test.testdata.union_testdata')

local test = tap.test('to avro schema')

box.cfg{wal_mode="none"}
test:plan(3)

testdata.init_spaces()
local meta = testdata.get_test_metadata()
local virtbox = vb.get_virtbox_for_accessor('space', {meta = meta})
testdata.fill_test_data(virtbox)

local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = meta.schemas,
    collections = meta.collections,
    service_fields = meta.service_fields,
    indexes = meta.indexes,
    accessor = 'space'
}, test_utils.test_conf_graphql_opts()))

local query = [[
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

local expected_avro_schema = [[
    type: record
    name: Query
    fields:
    - name: user_collection
      type:
        type: array
        items:
          type: record
          fields:
          - name: user_id
            type: string
          - name: name
            type: string
          - name: stuff
            type:
            - string
            - int
            - type: map
              values: int
            - type: record
              fields:
              - name: foo1
                type: string
              - name: foo2
                type: string
              name: Foo
              namespace: Query.user_collection
            - type: array
              items:
                type: map
                values: string
            - "null"
          name: user_collection
          namespace: Query
]]

expected_avro_schema = yaml.decode(expected_avro_schema)
local gql_query = gql_wrapper:compile(query)

local avros = gql_query:avro_schema()
test:is_deeply(avros, expected_avro_schema,
    "comparision between expected and generated (from query) avro-schemas")

local result_expected = [[
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
]]

result_expected = yaml.decode(result_expected)
local result = gql_query:execute({})

test:is_deeply(result.data, result_expected,
    'comparision between expected and actual query response')

local ok, compiled_avro, err
ok, compiled_avro = avro.create(avros)
assert(ok)

ok, err = avro.validate(compiled_avro, result.data)
assert(ok, err)

test:is(ok, true, 'query response validation by avro')

testdata.drop_spaces()

assert(test:check(), 'check plan')

os.exit()

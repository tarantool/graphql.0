#!/usr/bin/env tarantool
local fio = require('fio')
local yaml = require('yaml')
local avro = require('avro_schema')
local test = require('tap').test('to avro schema')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local data = require('test.testdata.nested_record_testdata')

local graphql = require('graphql')

box.cfg{wal_mode="none"}
test:plan(4)

data.init_spaces()
data.fill_test_data(box.space, data.meta)

local accessor = graphql.accessor_space.new({
    schemas = data.meta.schemas,
    collections = data.meta.collections,
    service_fields = data.meta.service_fields,
    indexes = data.meta.indexes,
})

local gql_wrapper = graphql.new({
    schemas = data.meta.schemas,
    collections = data.meta.collections,
    accessor = accessor,
})

local query = [[
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
local expected_avro_schema = [[
type: record
name: Query
fields:
- name: user
  type:
    type: array
    items:
      type: record
      fields:
      - name: uid
        type: long
      - name: p1
        type: string
      - name: p2
        type: string
      - name: nested
        type:
          type: record
          fields:
          - name: x
            type: long
          - name: y
            type: long
          name: nested
          namespace: Query.user
      name: user
      namespace: Query
]]
expected_avro_schema = yaml.decode(expected_avro_schema)
local gql_query = gql_wrapper:compile(query)
local variables = {
    uid = 1,
}

local avros = gql_query:avro_schema()

test:is_deeply(avros, expected_avro_schema, "generated avro schema")
local result_expected = [[
user:
- p2: p2 1
  p1: p1 1
  uid: 1
  nested:
    y: 2001
    x: 1001
]]
result_expected = yaml.decode(result_expected)
local result = gql_query:execute(variables)
test:is_deeply(result, result_expected, 'graphql query exec result')
local ok, ash, r, fs, _
ok, ash = avro.create(avros)
assert(ok)
ok, _ = avro.validate(ash, result)
assert(ok)
test:is(ok, true, 'gql result validation by avro')
ok, fs = avro.compile(ash)
assert(ok)
ok, r = fs.flatten(result)
assert(ok)
ok, r = fs.unflatten(r)
assert(ok)
test:is_deeply(r, result_expected, 'res = unflatten(flatten(res))')

os.exit(test:check() == true and 0 or 1)

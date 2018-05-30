#!/usr/bin/env tarantool
local fio = require('fio')
local yaml = require('yaml')
local avro = require('avro_schema')
local test = require('tap').test('to avro schema')
-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local testdata = require('test.testdata.nullable_index_testdata')

local graphql = require('graphql')

box.cfg{wal_mode="none"}
test:plan(4)

testdata.init_spaces()
local meta = testdata.get_test_metadata()
testdata.fill_test_data(box.space, meta)

local accessor = graphql.accessor_space.new({
    schemas = meta.schemas,
    collections = meta.collections,
    service_fields = meta.service_fields,
    indexes = meta.indexes,
})

local gql_wrapper = graphql.new({
    schemas = meta.schemas,
    collections = meta.collections,
    accessor = accessor,
})

-- We do not select `customer_balances` and `favorite_holidays` because thay are
-- is of `Map` type, which is not supported.
local query = [[
   query get_foo($id: String) {
        bar(id: $id) {
            id
            id_or_null_1
            id_or_null_2
            id_or_null_3
        }
    }
]]
local expected_avro_schema = [[
type: record
name: Query
fields:
- name: bar
  type:
    type: array
    items:
      type: record
      fields:
      - name: id
        type: string
      - name: id_or_null_1
        type: string*
      - name: id_or_null_2
        type: string*
      - name: id_or_null_3
        type: string*
      name: bar
      namespace: Query
]]
expected_avro_schema = yaml.decode(expected_avro_schema)
local gql_query = gql_wrapper:compile(query)
local variables = {
    id = '101',
}

local avros = gql_query:avro_schema()

test:is_deeply(avros, expected_avro_schema, "generated avro schema")
local result_expected = [[
bar:
- id_or_null_3: '101'
  id_or_null_2: '101'
  id: '101'
]]
result_expected = yaml.decode(result_expected)
local result = gql_query:execute(variables)
test:is_deeply(result, result_expected, 'graphql query exec result')
local ok, ash = avro.create(avros)
assert(ok, tostring(ash))
local ok, err = avro.validate(ash, result)
assert(ok, tostring(err))
test:is(ok, true, 'gql result validation by avro')
local ok, fs = avro.compile(ash)
assert(ok, tostring(fs))
local ok, r = fs.flatten(result)
assert(ok, tostring(r))
local ok, r = fs.unflatten(r)
assert(ok, tostring(r))
test:is_deeply(r, result_expected, 'res = unflatten(flatten(res))')

os.exit(test:check() == true and 0 or 1)

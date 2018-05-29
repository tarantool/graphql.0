#!/usr/bin/env tarantool
local fio = require('fio')
local yaml = require('yaml')
local avro = require('avro_schema')
local test = require('tap').test('to avro schema')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local testdata = require('test.testdata.array_and_map_testdata')

local graphql = require('graphql')

box.cfg{wal_mode="none"}
test:plan(4)

testdata.init_spaces()
testdata.fill_test_data()
local meta = testdata.get_test_metadata()

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
    query user_holidays($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            favorite_food
            user_balances {
                value
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
      - name: favorite_food
        type:
          type: array
          items: string
      - name: user_balances
        type:
          type: array
          items:
            type: record
            fields:
            - name: value
              type: int
            name: balance
            namespace: Query.user_collection
      name: user_collection
      namespace: Query
]]
expected_avro_schema = yaml.decode(expected_avro_schema)
local gql_query = gql_wrapper:compile(query)
local variables = {
    user_id = 'user_id_1',
}

local avros = gql_query:avro_schema()

test:is_deeply(avros, expected_avro_schema, "generated avro schema")
local result_expected = [[
user_collection:
- user_id: user_id_1
  user_balances:
  - value: 33
  - value: 44
  favorite_food:
  - meat
  - potato
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

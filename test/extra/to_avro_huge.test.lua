#!/usr/bin/env tarantool
local fio = require('fio')
local yaml = require('yaml')
local avro = require('avro_schema')
local data = require('test_data_user_order')
local test = require('tap').test('to avro schema')
-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local graphql = require('graphql')

box.cfg{wal_mode="none"}
test:plan(4)

data.init_spaces()
data.fill_test_data(box.space)

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
    query object_result_max($user_id: Int, $order_id: Int) {
        user_collection(id: $user_id) {
            id
            last_name
            first_name
            order_connection(limit: 1) {
                id
                user_id
                description
                order__order_item {
                    order_id
                    item_id
                    order_item__item{
                        id
                        name
                        description
                        price
                    }
                }
            }
        },
        order_collection(id: $order_id) {
            id
            description
            user_connection {
                id
                first_name
                last_name
                order_connection(limit: 1) {
                    id
                    order__order_item {
                        order_item__item {
                            name
                            price
                        }
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
      - name: order_connection
        type:
          type: array
          items:
            type: record
            fields:
            - name: user_id
              type: int
            - name: order__order_item
              type:
                type: array
                items:
                  type: record
                  fields:
                  - name: order_id
                    type: int
                  - name: item_id
                    type: int
                  - name: order_item__item
                    type:
                      type: record
                      fields:
                      - name: description
                        type: string
                      - name: price
                        type: string
                      - name: name
                        type: string
                      - name: id
                        type: int
                      name: item_collection
                      namespace: Query.user_collection.order_collection.order_item_collection
                  name: order_item_collection
                  namespace: Query.user_collection.order_collection
            - name: description
              type: string
            - name: id
              type: int
            name: order_collection
            namespace: Query.user_collection
      - name: last_name
        type: string
      - name: first_name
        type: string
      - name: id
        type: int
      name: user_collection
      namespace: Query
- name: order_collection
  type:
    type: array
    items:
      type: record
      fields:
      - name: user_connection
        type:
          type: record
          fields:
          - name: order_connection
            type:
              type: array
              items:
                type: record
                fields:
                - name: order__order_item
                  type:
                    type: array
                    items:
                      type: record
                      fields:
                      - name: order_item__item
                        type:
                          type: record
                          fields:
                          - name: name
                            type: string
                          - name: price
                            type: string
                          name: item_collection
                          namespace: Query.order_collection.user_collection.order_collection.order_item_collection
                      name: order_item_collection
                      namespace: Query.order_collection.user_collection.order_collection
                - name: id
                  type: int
                name: order_collection
                namespace: Query.order_collection.user_collection
          - name: last_name
            type: string
          - name: first_name
            type: string
          - name: id
            type: int
          name: user_collection
          namespace: Query.order_collection
      - name: id
        type: int
      - name: description
        type: string
      name: order_collection
      namespace: Query

]]
expected_avro_schema = yaml.decode(expected_avro_schema)
local gql_query = gql_wrapper:compile(query)
local variables = {
    user_id = 5,
    order_id = 20
}

local avros = gql_query:avro_schema()
test:is_deeply(avros, expected_avro_schema, "generated avro schema")
local result_expected = [[
user_collection:
- order_connection:
  - user_id: 5
    id: 11
    description: order of user 5
    order__order_item:
    - order_id: 1
      item_id: 11
      order_item__item:
        id: 11
        price: '9.74'
        name: Money
        description: lobortis ultrices. Vivamus rhoncus.
    - order_id: 29
      item_id: 11
      order_item__item:
        id: 11
        price: '9.74'
        name: Money
        description: lobortis ultrices. Vivamus rhoncus.
    - order_id: 30
      item_id: 11
      order_item__item:
        id: 11
        price: '9.74'
        name: Money
        description: lobortis ultrices. Vivamus rhoncus.
  last_name: user ln 5
  first_name: user fn 5
  id: 5
order_collection:
- description: order of user 6
  user_connection:
    order_connection:
    - id: 16
      order__order_item:
      - order_item__item:
          name: Cup
          price: '8.05'
      - order_item__item:
          name: Cup
          price: '8.05'
      - order_item__item:
          name: Cup
          price: '8.05'
    last_name: user ln 6
    first_name: user fn 6
    id: 6
  id: 20
]]
result_expected = yaml.decode(result_expected)
local result = gql_query:execute(variables)
test:is_deeply(result, result_expected, 'graphql qury exec result')
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
-- The test can fail if wrong avro-schema version is installed.
-- Please install avro-schema >= fea0ead9d1.
assert(ok)
test:is_deeply(r, result_expected, 'res = unflatten(flatten(res))')

os.exit(test:check() == true and 0 or 1)

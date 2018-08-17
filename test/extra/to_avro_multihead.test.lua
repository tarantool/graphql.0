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
local testdata = require('test.testdata.multihead_conn_with_nulls_testdata')

local test = tap.test('to avro schema')

test:plan(7)

box.cfg{}

testdata.init_spaces()
local meta = testdata.get_test_metadata()
testdata.fill_test_data(box.space, meta)

local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = meta.schemas,
    collections = meta.collections,
    service_fields = meta.service_fields,
    indexes = meta.indexes,
    accessor = 'space'
}, test_utils.test_conf_graphql_opts()))

local query = [[
    query obtainHeroes($hero_id: String) {
        hero_collection(hero_id: $hero_id) {
            hero_id
            hero_type
            banking_type
            hero_connection {
                ... on box_human_collection {
                    human_collection {
                        name
                    }
                }
                ... on box_starship_collection {
                    starship_collection {
                        model
                    }
                }
            }
            hero_banking_connection {
                ... on box_array_credit_account_collection {
                    credit_account_collection {
                        account_id
                        hero_banking_id
                    }
                }
                ... on box_array_dublon_account_collection {
                    dublon_account_collection {
                        account_id
                        hero_banking_id
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
    - name: hero_collection
      type:
        type: array
        items:
          type: record
          fields:
          - name: hero_id
            type: string
          - name: hero_type
            type: string*
          - name: banking_type
            type: string*
          - name: hero_connection
            type:
              type: record*
              name: results___hero_connection
              fields:
              - name: human_collection
                type:
                  type: record*
                  fields:
                  - name: name
                    type: string
                  name: human_collection
                  namespace: Query.hero_collection
              - name: starship_collection
                type:
                  type: record*
                  fields:
                  - name: model
                    type: string
                  name: starship_collection
                  namespace: Query.hero_collection
          - name: hero_banking_connection
            type:
              type: record*
              name: results___hero_banking_connection
              fields:
              - name: credit_account_collection
                type:
                  type: array*
                  items:
                    type: record
                    fields:
                    - name: account_id
                      type: string
                    - name: hero_banking_id
                      type: string
                    name: credit_account_collection
                    namespace: Query.hero_collection
              - name: dublon_account_collection
                type:
                  type: array*
                  items:
                    type: record
                    fields:
                    - name: account_id
                      type: string
                    - name: hero_banking_id
                      type: string
                    name: dublon_account_collection
                    namespace: Query.hero_collection
          name: hero_collection
          namespace: Query
]]

local gql_query = gql_wrapper:compile(query)
local avro_from_query = gql_query:avro_schema()

expected_avro_schema = yaml.decode(expected_avro_schema)

test:is_deeply(avro_from_query, expected_avro_schema,
    'comparision between expected and generated (from query) avro-schemas')

local ok, compiled_schema = avro.create(avro_from_query)
assert(ok, tostring(compiled_schema))

local variables_1 = {
    hero_id = 'hero_id_1'
}

local result_1 = gql_query:execute(variables_1)
result_1 = result_1.data
local result_expected_1 = yaml.decode([[
    hero_collection:
    - hero_id: hero_id_1
      hero_type: human
      hero_connection:
        human_collection:
          name: Luke
      banking_type: credit
      hero_banking_connection:
        credit_account_collection:
          - account_id: credit_account_id_1
            hero_banking_id: hero_banking_id_1
          - account_id: credit_account_id_2
            hero_banking_id: hero_banking_id_1
          - account_id: credit_account_id_3
            hero_banking_id: hero_banking_id_1
]])

test:is_deeply(result_1, result_expected_1,
    'comparision between expected and actual query response 1')

local ok, err = avro.validate(compiled_schema, result_1)
assert(ok, tostring(err))
test:is(ok, true, 'query response validation by avro 1')

local variables_2 = {
    hero_id = 'hero_id_2'
}

local result_2 = gql_query:execute(variables_2)
result_2 = result_2.data
local result_expected_2 = yaml.decode([[
    hero_collection:
    - hero_id: hero_id_2
      hero_type: starship
      hero_connection:
        starship_collection:
          model: Falcon-42
      banking_type: dublon
      hero_banking_connection:
        dublon_account_collection:
          - account_id: dublon_account_id_1
            hero_banking_id: hero_banking_id_2
          - account_id: dublon_account_id_2
            hero_banking_id: hero_banking_id_2
          - account_id: dublon_account_id_3
            hero_banking_id: hero_banking_id_2
]])

test:is_deeply(result_2, result_expected_2,
    'comparision between expected and actual query response 2')

local ok, err = avro.validate(compiled_schema, result_2)
assert(ok, tostring(err))
test:is(ok, true, 'query response validation by avro 2')

local variables_3 = {
    hero_id = 'hero_id_3'
}

local result_3 = gql_query:execute(variables_3)
result_3 = result_3.data

local result_expected_3 = yaml.decode([[
    hero_collection:
    - hero_id: hero_id_3
]])

test:is_deeply(result_3, result_expected_3,
    'comparision between expected and actual query response 3')

local ok, err = avro.validate(compiled_schema, result_3)
assert(ok, tostring(err))
test:is(ok, true, 'query response validation by avro 3')

testdata.drop_spaces()

assert(test:check(), 'check plan')

os.exit()

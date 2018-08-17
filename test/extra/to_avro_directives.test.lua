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
local common_testdata = require('test.testdata.common_testdata')
local union_testdata = require('test.testdata.union_testdata')
local multihead_testdata = require('test.testdata.multihead_conn_testdata')

local test = tap.test('to avro schema')

test:plan(15)

box.cfg{}

-- Common testdata

common_testdata.init_spaces()
local common_meta = common_testdata.get_test_metadata()
common_testdata.fill_test_data(box.space, common_meta)

local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = common_meta.schemas,
    collections = common_meta.collections,
    service_fields = common_meta.service_fields,
    indexes = common_meta.indexes,
    accessor = 'space'
}, test_utils.test_conf_graphql_opts()))

local common_query = [[
    query order_by_id($order_id: String, $include_description: Boolean,
            $skip_discount: Boolean, $include_user: Boolean,
            $include_user_first_name: Boolean, $user_id: String,
            $include_order_meta: Boolean) {
        order_collection(order_id: $order_id) {
            order_id
            description @include(if: $include_description)
            discount  @skip(if: $skip_discount)
            user_connection(user_id: "user_id_1") @include(if: $include_user) {
                user_id
                first_name @include(if: $include_user_first_name)
            }
            order_metainfo_connection(order_id: $order_id)
                    @skip(if: $include_order_meta) {
                order_metainfo_id
            }
        }
        user_collection(user_id: $user_id) @include(if: $include_user) {
            user_id
            first_name
        }
    }
]]

local expected_avro_schema = yaml.decode([[
    type: record
    name: Query
    fields:
    - name: order_collection
      type:
        type: array
        items:
          type: record
          fields:
          - name: order_id
            type: string
          - name: description
            type: string*
          - name: discount
            type: double*
          - name: user_connection
            type:
              type: record*
              fields:
              - name: user_id
                type: string
              - name: first_name
                type: string*
              name: user_collection
              namespace: Query.order_collection
          - name: order_metainfo_connection
            type:
              type: record*
              fields:
              - name: order_metainfo_id
                type: string
              name: order_metainfo_collection
              namespace: Query.order_collection
          name: order_collection
          namespace: Query
    - name: user_collection
      type:
        type: array*
        items:
          type: record
          fields:
          - name: user_id
            type: string
          - name: first_name
            type: string
          name: user_collection
          namespace: Query
]])

local gql_query = gql_wrapper:compile(common_query)

local avro_from_query = gql_query:avro_schema()
test:is_deeply(avro_from_query, expected_avro_schema,
    'comparision between expected and generated (from query) avro-schemas ' ..
    '- common')

local ok, compiled_schema = avro.create(avro_from_query)
assert(ok, tostring(compiled_schema))

local variables_1 = {
    order_id = 'order_id_1',
    user_id = 'user_id_1',
    include_description = true,
    skip_discount = false,
    include_user = true,
    include_user_first_name = false,
    include_order_meta = true
}

local result_1 = gql_query:execute(variables_1)
result_1 = result_1.data
local result_expected_1 = yaml.decode([[
    user_collection:
    - user_id: user_id_1
      first_name: Ivan
    order_collection:
    - order_id: order_id_1
      discount: 0
      description: first order of Ivan
      user_connection:
        user_id: user_id_1
]])

test:is_deeply(result_1, result_expected_1,
    'comparision between expected and actual query response - common 1')

local ok, err = avro.validate(compiled_schema, result_1)
assert(ok, tostring(err))
test:ok(ok, 'query response validation by avro - common 1')

local variables_2 = {
    order_id = 'order_id_1',
    user_id = 'user_id_1',
    include_description = false,
    skip_discount = true,
    include_user = true,
    include_user_first_name = true,
    include_order_meta = false
}

local result_2 = gql_query:execute(variables_2)
result_2 = result_2.data
local result_expected_2 = yaml.decode([[
    user_collection:
    - user_id: user_id_1
      first_name: Ivan
    order_collection:
    - order_id: order_id_1
      user_connection:
        user_id: user_id_1
        first_name: Ivan
      order_metainfo_connection:
        order_metainfo_id: order_metainfo_id_1
]])

test:is_deeply(result_2, result_expected_2,
    'comparision between expected and actual query response - common 2')

local ok, err = avro.validate(compiled_schema, result_2)
assert(ok, tostring(err))
test:ok(ok, 'query response validation by avro - common 2')

common_testdata.drop_spaces()

-- Union testdata

union_testdata.init_spaces()
local union_meta = union_testdata.get_test_metadata()
union_testdata.fill_test_data(box.space, union_meta)

local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = union_meta.schemas,
    collections = union_meta.collections,
    service_fields = union_meta.service_fields,
    indexes = union_meta.indexes,
    accessor = 'space'
}, test_utils.test_conf_graphql_opts()))

local union_query = [[
    query user_collection ($include_stuff: Boolean) {
        user_collection {
            user_id
            name
            stuff @include(if: $include_stuff) {
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

local expected_avro_schema = yaml.decode([[
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
]])


local gql_query = gql_wrapper:compile(union_query)

local avro_from_query = gql_query:avro_schema()
test:is_deeply(avro_from_query, expected_avro_schema,
    'comparision between expected and generated (from query) avro-schemas ' ..
    '- union')

local ok, compiled_schema = avro.create(avro_from_query)
assert(ok, tostring(compiled_schema))

local variables_1 = {
    include_stuff = true
}

local result_1 = gql_query:execute(variables_1)
result_1 = result_1.data
local result_expected_1 = yaml.decode([[
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
]])

test:is_deeply(result_1, result_expected_1,
    'comparision between expected and actual query response - union 1')

local ok, err = avro.validate(compiled_schema, result_1)
assert(ok, tostring(err))
test:ok(ok, 'query response validation by avro - union 1')

local variables_2 = {
    include_stuff = false
}

local result_2 = gql_query:execute(variables_2)
result_2 = result_2.data
local result_expected_2 = yaml.decode([[
    user_collection:
    - user_id: user_id_1
      name: Nobody
    - user_id: user_id_2
      name: Zlata
    - user_id: user_id_3
      name: Ivan
    - user_id: user_id_4
      name: Jane
    - user_id: user_id_5
      name: Dan
    - user_id: user_id_6
      name: Max
]])

test:is_deeply(result_2, result_expected_2,
    'comparision between expected and actual query response - union 2')

local ok, err = avro.validate(compiled_schema, result_2)
assert(ok, tostring(err))
test:ok(ok, 'query response validation by avro - union 2')

union_testdata.drop_spaces()

-- Multi-head connection testdata

multihead_testdata.init_spaces()
local multihead_meta = multihead_testdata.get_test_metadata()
multihead_testdata.fill_test_data(box.space, multihead_meta)

local gql_wrapper = graphql.new(utils.merge_tables({
    schemas = multihead_meta.schemas,
    collections = multihead_meta.collections,
    service_fields = multihead_meta.service_fields,
    indexes = multihead_meta.indexes,
    accessor = 'space'
}, test_utils.test_conf_graphql_opts()))

local multihead_query = [[
    query obtainHeroes($hero_id: String, $include_connections: Boolean) {
        hero_collection(hero_id: $hero_id) {
            hero_id
            hero_type
            banking_type
            hero_connection @include(if: $include_connections) {
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
            hero_banking_connection @include(if: $include_connections) {
                ... on box_array_credit_account_collection {
                    credit_account_collection {
                        account_id
                        hero_id
                    }
                }
                ... on box_array_dublon_account_collection {
                    dublon_account_collection {
                        account_id
                        hero_id
                    }
                }
            }
        }
    }
]]

local expected_avro_schema = yaml.decode([[
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
            type: string
          - name: banking_type
            type: string
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
                    - name: hero_id
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
                    - name: hero_id
                      type: string
                    name: dublon_account_collection
                    namespace: Query.hero_collection
          name: hero_collection
          namespace: Query
]])

local gql_query = gql_wrapper:compile(multihead_query)

local avro_from_query = gql_query:avro_schema()

test:is_deeply(avro_from_query, expected_avro_schema,
    'comparision between expected and generated (from query) avro-schemas ' ..
    '- multihead')

local ok, compiled_schema = avro.create(avro_from_query)
assert(ok, tostring(compiled_schema))

local variables_1 = {
    hero_id = 'hero_id_1',
    include_connections = true
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
              hero_id: hero_id_1
            - account_id: credit_account_id_2
              hero_id: hero_id_1
            - account_id: credit_account_id_3
              hero_id: hero_id_1
]])

test:is_deeply(result_1, result_expected_1,
    'comparision between expected and actual query response - multihead 1')

local ok, err = avro.validate(compiled_schema, result_1)
assert(ok, tostring(err))
test:ok(ok, 'query response validation by avro - multihead 1')

local variables_2 = {
    hero_id = 'hero_id_2',
    include_connections = false
}

local result_2 = gql_query:execute(variables_2)
result_2 = result_2.data
local result_expected_2 = yaml.decode([[
    hero_collection:
    - hero_id: hero_id_2
      hero_type: starship
      banking_type: dublon
]])

test:is_deeply(result_2, result_expected_2,
    'comparision between expected and actual query response - multihead 2')

local ok, err = avro.validate(compiled_schema, result_2)
assert(ok, tostring(err))
test:ok(ok, 'query response validation by avro - multihead 2')

multihead_testdata.drop_spaces()

assert(test:check(), 'check plan')

os.exit()

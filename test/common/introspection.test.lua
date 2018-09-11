#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')

box.cfg({})

local exp_result_avro_schema_3

local function run_queries(gql_wrapper)
    local query = [[
        query IntrospectionQuery {
          __schema {
            queryType { name }
            mutationType { name }
            subscriptionType { name }
            types {
              ...FullType
            }
            directives {
              name
              description
              locations
              args {
                ...InputValue
              }
            }
          }
        }

        fragment FullType on __Type {
          kind
          name
          description
          fields(includeDeprecated: true) {
            name
            description
            args {
              ...InputValue
            }
            type {
              ...TypeRef
            }
            isDeprecated
            deprecationReason
          }
          inputFields {
            ...InputValue
          }
          interfaces {
            ...TypeRef
          }
          enumValues(includeDeprecated: true) {
            name
            description
            isDeprecated
            deprecationReason
          }
          possibleTypes {
            ...TypeRef
          }
        }

        fragment InputValue on __InputValue {
          name
          description
          type { ...TypeRef }
          defaultValue
        }

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
    ]]

    -- Note: introspection for avro-schema-2* is different (doesn't have
    -- mutation arguments)
    -- Test only avro-schema-3* because:
    -- 1. introspection for avro-schema-2* is build in the same way
    -- 2. diffs on every introspection change are huge and hard to analyze
    -- 3. it takes a noticeable time to prepare each ret_val_etalon after
    --    a change
    local test = tap.test('introspection')
    if test_utils.major_avro_schema_version() == 2 then
        test:plan(0)
    else -- avro-schema-3*
        test:plan(1)
        test_utils.show_trace(function()
            local gql_query = gql_wrapper:compile(query)
            local result = gql_query:execute({})
            test:is_deeply(result.data, exp_result_avro_schema_3,
                'introspection query')
        end)
    end

    assert(test:check(), 'check plan')
end

-- luacheck: push max line length 160
exp_result_avro_schema_3 = yaml.decode(([[
---
__schema:
  mutationType:
    name: results___Mutation
  types:
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___store___store___address___address
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: state
    - type:
        name: String
        kind: SCALAR
      name: zip
    - type:
        name: String
        kind: SCALAR
      name: city
    - type:
        name: String
        kind: SCALAR
      name: street
  - description: generated from avro-schema for store
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___store___store
    inputFields:
    - type:
        name: arguments___order_metainfo_collection___store___store___address___address
        kind: INPUT_OBJECT
      name: address
    - type:
        name: String
        kind: SCALAR
      name: name
    - type:
        name: arguments___order_metainfo_collection___store___store___second_address___address
        kind: INPUT_OBJECT
      name: second_address
  - description: generated from avro-schema for store
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store
    inputFields:
    - type:
        name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___address___address
        kind: INPUT_OBJECT
      name: address
    - type:
        name: String
        kind: SCALAR
      name: name
    - type:
        name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___second_address___address
        kind: INPUT_OBJECT
      name: second_address
  - name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id
    kind: SCALAR
  - fields:
    - isDeprecated: false
      args:
      - type:
          name: order_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: Boolean
          kind: SCALAR
        name: in_stock
      - type:
          name: String
          kind: SCALAR
        name: order_id
      - type:
          name: Boolean
          kind: SCALAR
        name: delete
      - type:
          name: order_metainfo_connection
          kind: INPUT_OBJECT
        name: order_metainfo_connection
      - type:
          name: user_connection
          kind: INPUT_OBJECT
        name: user_connection
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: String
          kind: SCALAR
        name: user_id
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: order_collection_update
          kind: INPUT_OBJECT
        name: update
      - type:
          name: String
          kind: SCALAR
        name: description
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___order_collection
              kind: OBJECT
      name: order_connection
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: first_name
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: user_id
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: last_name
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: middle_name
    kind: OBJECT
    interfaces: []
    name: results___user_collection
    description: generated from avro-schema for user
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: description
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: Double
          kind: SCALAR
      name: price
    - isDeprecated: false
      args:
      - type:
          name: Boolean
          kind: SCALAR
        name: delete
      - type:
          name: String
          kind: SCALAR
        name: metainfo
      - type:
          name: String
          kind: SCALAR
        name: order_metainfo_id
      - type:
          name: String
          kind: SCALAR
        name: order_metainfo_id_copy
      - type:
          name: String
          kind: SCALAR
        name: order_id
      - type:
          name: order_metainfo_collection_update
          kind: INPUT_OBJECT
        name: update
      - type:
          name: arguments___order_metainfo_collection___store___store
          kind: INPUT_OBJECT
        name: store
      type:
        name: results___order_metainfo_collection
        kind: OBJECT
      name: order_metainfo_connection
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: Boolean
          kind: SCALAR
      name: in_stock
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: user_id
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_id
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: Float
          kind: SCALAR
      name: discount
    - isDeprecated: false
      args:
      - type:
          name: order_connection
          kind: INPUT_OBJECT
        name: order_connection
      - type:
          name: String
          kind: SCALAR
        name: first_name
      - type:
          name: Boolean
          kind: SCALAR
        name: delete
      - type:
          name: String
          kind: SCALAR
        name: user_id
      - type:
          name: String
          kind: SCALAR
        name: last_name
      - type:
          name: user_collection_update
          kind: INPUT_OBJECT
        name: update
      - type:
          name: String
          kind: SCALAR
        name: middle_name
      type:
        name: results___user_collection
        kind: OBJECT
      name: user_connection
    kind: OBJECT
    interfaces: []
    name: results___order_collection
    description: generated from avro-schema for order
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_metainfo_id
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_metainfo_id_copy
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: metainfo
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_id
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: results___order_metainfo_collection___store___store
          kind: OBJECT
      name: store
    kind: OBJECT
    interfaces: []
    name: results___order_metainfo_collection
    description: generated from avro-schema for order_metainfo
  - enumValues:
    - isDeprecated: false
      name: FRAGMENT_SPREAD
      description: Location adjacent to a fragment spread.
    - isDeprecated: false
      name: MUTATION
      description: Location adjacent to a mutation operation.
    - isDeprecated: false
      name: FRAGMENT_DEFINITION
      description: Location adjacent to a fragment definition.
    - isDeprecated: false
      name: FIELD
      description: Location adjacent to a field.
    - isDeprecated: false
      name: QUERY
      description: Location adjacent to a query operation.
    - isDeprecated: false
      name: INLINE_FRAGMENT
      description: Location adjacent to an inline fragment.
    description: A Directive can be adjacent to many parts of the GraphQL language,
      a __DirectiveLocation describes one such possible adjacencies.
    name: __DirectiveLocation
    kind: ENUM
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: results___order_metainfo_collection___store___store___address___address
          kind: OBJECT
      name: address
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: results___order_metainfo_collection___store___store___second_address___address
          kind: OBJECT
      name: second_address
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: String
              kind: SCALAR
      name: tags
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: results___order_metainfo_collection___store___store___external_id___external_id
          kind: UNION
      name: external_id
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: name
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: results___order_metainfo_collection___store___store___parametrized_tags___Map
          kind: SCALAR
      name: parametrized_tags
    kind: OBJECT
    interfaces: []
    name: results___order_metainfo_collection___store___store
    description: generated from avro-schema for store
  - fields:
    - isDeprecated: false
      args:
      - type:
          name: user_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: String
          kind: SCALAR
        name: middle_name
      - type:
          name: order_connection
          kind: INPUT_OBJECT
        name: order_connection
      - type:
          name: Boolean
          kind: SCALAR
        name: delete
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: String
          kind: SCALAR
        name: first_name
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: String
          kind: SCALAR
        name: user_id
      - type:
          name: String
          kind: SCALAR
        name: last_name
      - type:
          name: user_collection_update
          kind: INPUT_OBJECT
        name: update
      - type:
          name: user_collection_insert
          kind: INPUT_OBJECT
        name: insert
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___user_collection
              kind: OBJECT
      name: user_collection
    - isDeprecated: false
      args:
      - type:
          name: order_metainfo_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: String
          kind: SCALAR
        name: order_metainfo_id
      - type:
          name: String
          kind: SCALAR
        name: order_id
      - type:
          name: Boolean
          kind: SCALAR
        name: delete
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: arguments___order_metainfo_collection___store___store
          kind: INPUT_OBJECT
        name: store
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: String
          kind: SCALAR
        name: order_metainfo_id_copy
      - type:
          name: String
          kind: SCALAR
        name: metainfo
      - type:
          name: order_metainfo_collection_update
          kind: INPUT_OBJECT
        name: update
      - type:
          name: order_metainfo_collection_insert
          kind: INPUT_OBJECT
        name: insert
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___order_metainfo_collection
              kind: OBJECT
      name: order_metainfo_collection
    - isDeprecated: false
      args:
      - type:
          name: order_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: Boolean
          kind: SCALAR
        name: in_stock
      - type:
          name: String
          kind: SCALAR
        name: order_id
      - type:
          name: String
          kind: SCALAR
        name: description
      - type:
          name: Boolean
          kind: SCALAR
        name: delete
      - type:
          name: order_metainfo_connection
          kind: INPUT_OBJECT
        name: order_metainfo_connection
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: String
          kind: SCALAR
        name: user_id
      - type:
          name: order_collection_insert
          kind: INPUT_OBJECT
        name: insert
      - type:
          name: order_collection_update
          kind: INPUT_OBJECT
        name: update
      - type:
          name: user_connection
          kind: INPUT_OBJECT
        name: user_connection
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___order_collection
              kind: OBJECT
      name: order_collection
    kind: OBJECT
    interfaces: []
    name: results___Mutation
    description: generated from avro-schema for Mutation
  - fields:
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: description
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: __DirectiveLocation
              kind: ENUM
      name: locations
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: name
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: __InputValue
              kind: OBJECT
      name: args
    kind: OBJECT
    interfaces: []
    name: __Directive
    description: A Directive provides a way to describe alternate runtime execution
      and type validation behavior in a GraphQL document. In some cases, you need
      to provide options to alter GraphQL’s execution behavior in ways field arguments
      will not suffice, such as conditionally including or skipping a field. Directives
      provide this by describing additional information to the executor.
  - enumValues:
    - isDeprecated: false
      name: ENUM
      description: Indicates this type is an enum. `enumValues` is a valid field.
    - isDeprecated: false
      name: INTERFACE
      description: Indicates this type is an interface. `fields` and `possibleTypes`
        are valid fields.
    - isDeprecated: false
      name: LIST
      description: Indicates this type is a list. `ofType` is a valid field.
    - isDeprecated: false
      name: UNION
      description: Indicates this type is a union. `possibleTypes` is a valid field.
    - isDeprecated: false
      name: NON_NULL
      description: Indicates this type is a non-null. `ofType` is a valid field.
    - isDeprecated: false
      name: SCALAR
      description: Indicates this type is a scalar.
    - isDeprecated: false
      name: INPUT_OBJECT
      description: Indicates this type is an input object. `inputFields` is a valid
        field.
    - isDeprecated: false
      name: OBJECT
      description: Indicates this type is an object. `fields` and `interfaces` are
        valid fields.
    description: An enum describing what kind of type a given `__Type` is.
    name: __TypeKind
    kind: ENUM
  - description: generated from avro-schema for order_collection_insert
    kind: INPUT_OBJECT
    name: order_collection_insert
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: Double
          kind: SCALAR
      name: price
    - type:
        name: Boolean
        kind: SCALAR
      name: in_stock
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: user_id
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_id
    - type:
        kind: NON_NULL
        ofType:
          name: Float
          kind: SCALAR
      name: discount
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: description
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: Boolean
          kind: SCALAR
      name: isDeprecated
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: deprecationReason
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: __InputValue
              kind: OBJECT
      name: args
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: __Type
          kind: OBJECT
      name: type
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: name
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: description
    kind: OBJECT
    interfaces: []
    name: __Field
    description: Object and Interface types are described by a list of Fields, each
      of which has a name, potentially a list of arguments, and a return type.
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: Int
          kind: SCALAR
      name: int
    kind: OBJECT
    interfaces: []
    name: Int_box
    description: Box (wrapper) around union variant
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: __Type
          kind: OBJECT
      name: type
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: description
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: name
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: defaultValue
      description: A GraphQL-formatted string representing the default value for this
        input value.
    kind: OBJECT
    interfaces: []
    name: __InputValue
    description: Arguments provided to Fields or Directives and the input fields of
      an InputObject are represented as Input Values which describe their type and
      optionally a default value.
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___store___store___second_address___address
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: state
    - type:
        name: String
        kind: SCALAR
      name: zip
    - type:
        name: String
        kind: SCALAR
      name: city
    - type:
        name: String
        kind: SCALAR
      name: street
  - possibleTypes:
    - name: Int_box
      kind: OBJECT
    - name: String_box
      kind: OBJECT
    name: results___order_metainfo_collection___store___store___external_id___external_id
    kind: UNION
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___address___address
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: state
    - type:
        name: String
        kind: SCALAR
      name: zip
    - type:
        name: String
        kind: SCALAR
      name: city
    - type:
        name: String
        kind: SCALAR
      name: street
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___second_address___address
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: state
    - type:
        name: String
        kind: SCALAR
      name: zip
    - type:
        name: String
        kind: SCALAR
      name: city
    - type:
        name: String
        kind: SCALAR
      name: street
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: string
    kind: OBJECT
    interfaces: []
    name: String_box
    description: Box (wrapper) around union variant
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: Boolean
          kind: SCALAR
      name: isDeprecated
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: name
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: deprecationReason
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: description
    kind: OBJECT
    interfaces: []
    name: __EnumValue
    description: One possible value for a given Enum. Enum values are unique values,
      not a placeholder for a string or numeric value. However an Enum value is returned
      in a JSON response as a string.
  - description: generated from avro-schema for store
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___address___address
          kind: INPUT_OBJECT
      name: address
    - type:
        kind: NON_NULL
        ofType:
          name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___second_address___address
          kind: INPUT_OBJECT
      name: second_address
    - type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: String
              kind: SCALAR
      name: tags
    - type:
        kind: NON_NULL
        ofType:
          name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id
          kind: SCALAR
      name: external_id
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: name
    - type:
        kind: NON_NULL
        ofType:
          name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___parametrized_tags___InputMap
          kind: SCALAR
      name: parametrized_tags
  - description: Box (wrapper) around union variant
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id___Int_box
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: Int
          kind: SCALAR
      name: int
  - description: generated from the connection "user_connection" of collection "order_collection"
      using collection "user_collection"
    kind: INPUT_OBJECT
    name: user_connection
    inputFields:
    - type:
        name: order_connection
        kind: INPUT_OBJECT
      name: order_connection
    - type:
        name: String
        kind: SCALAR
      name: first_name
    - type:
        name: String
        kind: SCALAR
      name: user_id
    - type:
        name: String
        kind: SCALAR
      name: last_name
    - type:
        name: String
        kind: SCALAR
      name: middle_name
  - name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___parametrized_tags___InputMap
    kind: SCALAR
  - description: Box (wrapper) around union variant
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id___String_box
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: string
  - description: Map is a dictionary with string keys and values of arbitrary but
      same among all values type
    name: results___order_metainfo_collection___store___store___parametrized_tags___Map
    kind: SCALAR
  - description: generated from avro-schema for order_collection_pcre
    kind: INPUT_OBJECT
    name: order_collection_pcre
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: user_id
    - type:
        name: String
        kind: SCALAR
      name: order_id
    - type:
        name: String
        kind: SCALAR
      name: description
  - fields:
    - isDeprecated: false
      args:
      - type:
          name: order_connection
          kind: INPUT_OBJECT
        name: order_connection
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: String
          kind: SCALAR
        name: first_name
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: String
          kind: SCALAR
        name: user_id
      - type:
          name: String
          kind: SCALAR
        name: last_name
      - type:
          name: user_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: String
          kind: SCALAR
        name: middle_name
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___user_collection
              kind: OBJECT
      name: user_collection
    - isDeprecated: false
      args:
      - type:
          name: order_metainfo_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: String
          kind: SCALAR
        name: order_metainfo_id
      - type:
          name: String
          kind: SCALAR
        name: order_metainfo_id_copy
      - type:
          name: String
          kind: SCALAR
        name: metainfo
      - type:
          name: String
          kind: SCALAR
        name: order_id
      - type:
          name: arguments___order_metainfo_collection___store___store
          kind: INPUT_OBJECT
        name: store
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___order_metainfo_collection
              kind: OBJECT
      name: order_metainfo_collection
    - isDeprecated: false
      args:
      - type:
          name: order_collection_pcre
          kind: INPUT_OBJECT
        name: pcre
      - type:
          name: Boolean
          kind: SCALAR
        name: in_stock
      - type:
          name: String
          kind: SCALAR
        name: order_id
      - type:
          name: order_metainfo_connection
          kind: INPUT_OBJECT
        name: order_metainfo_connection
      - type:
          name: String
          kind: SCALAR
        name: description
      - type:
          name: String
          kind: SCALAR
        name: user_id
      - type:
          name: String
          kind: SCALAR
        name: offset
      - type:
          name: Int
          kind: SCALAR
        name: limit
      - type:
          name: user_connection
          kind: INPUT_OBJECT
        name: user_connection
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: results___order_collection
              kind: OBJECT
      name: order_collection
    kind: OBJECT
    interfaces: []
    name: results___Query
    description: generated from avro-schema for Query
  - fields:
    - isDeprecated: false
      args: []
      type:
        name: __Type
        kind: OBJECT
      name: mutationType
      description: If this server supports mutation, the type that mutation operations
        will be rooted at.
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: __Type
          kind: OBJECT
      name: queryType
      description: The type that query operations will be rooted at.
    - isDeprecated: false
      args: []
      type:
        name: __Type
        kind: OBJECT
      name: subscriptionType
      description: If this server supports subscriptions, the type that subscription
        operations will be rooted at.
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: __Type
              kind: OBJECT
      name: types
      description: A list of all types supported by this server.
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          kind: LIST
          ofType:
            kind: NON_NULL
            ofType:
              name: __Directive
              kind: OBJECT
      name: directives
      description: A list of all directives supported by this server.
    kind: OBJECT
    interfaces: []
    name: __Schema
    description: A GraphQL Schema defines the capabilities of a GraphQL server. It
      exposes all available types and directives on the server, as well as the entry
      points for query and mutation operations.
  - description: generated from avro-schema for user_collection_insert
    kind: INPUT_OBJECT
    name: user_collection_insert
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: user_id
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: last_name
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: first_name
    - type:
        name: String
        kind: SCALAR
      name: middle_name
  - name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id
    kind: SCALAR
  - description: Box (wrapper) around union variant
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id___Int_box
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: Int
          kind: SCALAR
      name: int
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___second_address___address
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: state
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: zip
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: city
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: street
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___address___address
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: state
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: zip
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: city
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: street
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: state
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: zip
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: city
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: street
    kind: OBJECT
    interfaces: []
    name: results___order_metainfo_collection___store___store___second_address___address
    description: generated from avro-schema for address
  - fields:
    - isDeprecated: false
      args: []
      type:
        name: __Type
        kind: OBJECT
      name: ofType
    - isDeprecated: false
      args:
      - type:
          name: Boolean
          kind: SCALAR
        name: includeDeprecated
        defaultValue: 'false'
      type:
        kind: LIST
        ofType:
          kind: NON_NULL
          ofType:
            name: __EnumValue
            kind: OBJECT
      name: enumValues
    - isDeprecated: false
      args: []
      type:
        kind: LIST
        ofType:
          kind: NON_NULL
          ofType:
            name: __InputValue
            kind: OBJECT
      name: inputFields
    - isDeprecated: false
      args:
      - type:
          name: Boolean
          kind: SCALAR
        name: includeDeprecated
        defaultValue: 'false'
      type:
        kind: LIST
        ofType:
          kind: NON_NULL
          ofType:
            name: __Field
            kind: OBJECT
      name: fields
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: __TypeKind
          kind: ENUM
      name: kind
    - isDeprecated: false
      args: []
      type:
        kind: LIST
        ofType:
          kind: NON_NULL
          ofType:
            name: __Type
            kind: OBJECT
      name: interfaces
    - isDeprecated: false
      args: []
      type:
        kind: LIST
        ofType:
          kind: NON_NULL
          ofType:
            name: __Type
            kind: OBJECT
      name: possibleTypes
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: name
    - isDeprecated: false
      args: []
      type:
        name: String
        kind: SCALAR
      name: description
    kind: OBJECT
    interfaces: []
    name: __Type
    description: The fundamental unit of any GraphQL Schema is the type. There are
      many kinds of types in GraphQL as represented by the `__TypeKind` enum. Depending
      on the kind of a type, certain fields describe information about that type.
      Scalar types provide no information beyond a name and description, while Enum
      types provide their values. Object and Interface types provide the fields they
      describe. Abstract types, Union and Interface, provide the Object types possible
      at runtime. List and NonNull types compose other types.
  - description: generated from avro-schema for order_metainfo_collection_insert
    kind: INPUT_OBJECT
    name: order_metainfo_collection_insert
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_metainfo_id
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_metainfo_id_copy
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: metainfo
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: order_id
    - type:
        kind: NON_NULL
        ofType:
          name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store
          kind: INPUT_OBJECT
      name: store
  - fields:
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: state
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: zip
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: city
    - isDeprecated: false
      args: []
      type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: street
    kind: OBJECT
    interfaces: []
    name: results___order_metainfo_collection___store___store___address___address
    description: generated from avro-schema for address
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___second_address___address
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: state
    - type:
        name: String
        kind: SCALAR
      name: zip
    - type:
        name: String
        kind: SCALAR
      name: city
    - type:
        name: String
        kind: SCALAR
      name: street
  - description: generated from avro-schema for address
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___address___address
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: state
    - type:
        name: String
        kind: SCALAR
      name: zip
    - type:
        name: String
        kind: SCALAR
      name: city
    - type:
        name: String
        kind: SCALAR
      name: street
  - description: generated from avro-schema for order_metainfo_collection_pcre
    kind: INPUT_OBJECT
    name: order_metainfo_collection_pcre
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: order_metainfo_id
    - type:
        name: String
        kind: SCALAR
      name: order_metainfo_id_copy
    - type:
        name: String
        kind: SCALAR
      name: metainfo
    - type:
        name: String
        kind: SCALAR
      name: order_id
    - type:
        name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store
        kind: INPUT_OBJECT
      name: store
  - description: generated from avro-schema for user_collection_pcre
    kind: INPUT_OBJECT
    name: user_collection_pcre
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: user_id
    - type:
        name: String
        kind: SCALAR
      name: last_name
    - type:
        name: String
        kind: SCALAR
      name: first_name
    - type:
        name: String
        kind: SCALAR
      name: middle_name
  - name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___parametrized_tags___InputMap
    kind: SCALAR
  - description: generated from avro-schema for user_collection_update
    kind: INPUT_OBJECT
    name: user_collection_update
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: last_name
    - type:
        name: String
        kind: SCALAR
      name: first_name
    - type:
        name: String
        kind: SCALAR
      name: middle_name
  - name: Double
    kind: SCALAR
  - description: The `Int` scalar type represents non-fractional signed whole numeric
      values. Int can represent values between -(2^31) and 2^31 - 1.
    name: Int
    kind: SCALAR
  - description: generated from avro-schema for order_metainfo_collection_update
    kind: INPUT_OBJECT
    name: order_metainfo_collection_update
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: order_metainfo_id_copy
    - type:
        name: String
        kind: SCALAR
      name: metainfo
    - type:
        name: String
        kind: SCALAR
      name: order_id
    - type:
        name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store
        kind: INPUT_OBJECT
      name: store
  - description: generated from the connection "order_connection" of collection "user_collection"
      using collection "order_collection"
    kind: INPUT_OBJECT
    name: order_connection
    inputFields:
    - type:
        name: order_metainfo_connection
        kind: INPUT_OBJECT
      name: order_metainfo_connection
    - type:
        name: Boolean
        kind: SCALAR
      name: in_stock
    - type:
        name: String
        kind: SCALAR
      name: user_id
    - type:
        name: String
        kind: SCALAR
      name: order_id
    - type:
        name: user_connection
        kind: INPUT_OBJECT
      name: user_connection
    - type:
        name: String
        kind: SCALAR
      name: description
  - description: The `Boolean` scalar type represents `true` or `false`.
    name: Boolean
    kind: SCALAR
  - name: Float
    kind: SCALAR
  - description: The `String` scalar type represents textual data, represented as
      UTF-8 character sequences. The String type is most often used by GraphQL to
      represent free-form human-readable text.
    name: String
    kind: SCALAR
  - description: generated from the connection "order_metainfo_connection" of collection
      "order_collection" using collection "order_metainfo_collection"
    kind: INPUT_OBJECT
    name: order_metainfo_connection
    inputFields:
    - type:
        name: String
        kind: SCALAR
      name: order_metainfo_id
    - type:
        name: String
        kind: SCALAR
      name: order_metainfo_id_copy
    - type:
        name: String
        kind: SCALAR
      name: metainfo
    - type:
        name: String
        kind: SCALAR
      name: order_id
    - type:
        name: arguments___order_metainfo_collection___store___store
        kind: INPUT_OBJECT
      name: store
  - description: Box (wrapper) around union variant
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id___String_box
    inputFields:
    - type:
        kind: NON_NULL
        ofType:
          name: String
          kind: SCALAR
      name: string
  - description: generated from avro-schema for store
    kind: INPUT_OBJECT
    name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store
    inputFields:
    - type:
        name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___address___address
        kind: INPUT_OBJECT
      name: address
    - type:
        name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___second_address___address
        kind: INPUT_OBJECT
      name: second_address
    - type:
        kind: LIST
        ofType:
          kind: NON_NULL
          ofType:
            name: String
            kind: SCALAR
      name: tags
    - type:
        name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id
        kind: SCALAR
      name: external_id
    - type:
        name: String
        kind: SCALAR
      name: name
    - type:
        name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___parametrized_tags___InputMap
        kind: SCALAR
      name: parametrized_tags
  - description: generated from avro-schema for order_collection_update
    kind: INPUT_OBJECT
    name: order_collection_update
    inputFields:
    - type:
        name: Double
        kind: SCALAR
      name: price
    - type:
        name: Boolean
        kind: SCALAR
      name: in_stock
    - type:
        name: String
        kind: SCALAR
      name: user_id
    - type:
        name: Float
        kind: SCALAR
      name: discount
    - type:
        name: String
        kind: SCALAR
      name: description
  queryType:
    name: results___Query
  directives:
  - description: Directs the executor to include this field or fragment only when
      the `if` argument is true.
    locations:
    - FIELD
    - FRAGMENT_SPREAD
    - INLINE_FRAGMENT
    name: include
    args:
    - type:
        kind: NON_NULL
        ofType:
          name: Boolean
          kind: SCALAR
      name: if
      description: Included when true.
  - description: Directs the executor to skip this field or fragment when the `if`
      argument is true.
    locations:
    - FIELD
    - FRAGMENT_SPREAD
    - INLINE_FRAGMENT
    name: skip
    args:
    - type:
        kind: NON_NULL
        ofType:
          name: Boolean
          kind: SCALAR
      name: if
      description: Skipped when true.
]]):strip())
-- luacheck: pop

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

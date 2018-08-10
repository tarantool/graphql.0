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

local function run_queries(gql_wrapper)
    local test = tap.test('introspection')
    test:plan(1)


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

    -- luacheck: push max line length 156
    local exp_result_avro_schema_3 = yaml.decode(([[
        ---
        __schema:
          mutationType:
            name: results___Mutation
          types:
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___store___store
            description: generated from avro-schema for store
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store
            description: generated from avro-schema for store
          - name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id
            kind: SCALAR
          - interfaces: &0 []
            fields:
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: order_connection
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: first_name
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: user_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: last_name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: middle_name
            kind: OBJECT
            name: results___user_collection
            description: generated from avro-schema for user
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: description
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Double
                  kind: SCALAR
                kind: NON_NULL
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
              args: *0
              type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: in_stock
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: user_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Float
                  kind: SCALAR
                kind: NON_NULL
              name: discount
            - isDeprecated: false
              args:
              - type:
                  name: Boolean
                  kind: SCALAR
                name: delete
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
            name: results___order_collection
            description: generated from avro-schema for order
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_metainfo_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_metainfo_id_copy
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: metainfo
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store
                  kind: OBJECT
                kind: NON_NULL
              name: store
            kind: OBJECT
            name: results___order_metainfo_collection
            description: generated from avro-schema for order_metainfo
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___address___address
                  kind: OBJECT
                kind: NON_NULL
              name: address
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___second_address___address
                  kind: OBJECT
                kind: NON_NULL
              name: second_address
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: String
                      kind: SCALAR
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: tags
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___external_id___external_id
                  kind: UNION
                kind: NON_NULL
              name: external_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___parametrized_tags___Map
                  kind: SCALAR
                kind: NON_NULL
              name: parametrized_tags
            kind: OBJECT
            name: results___order_metainfo_collection___store___store
            description: generated from avro-schema for store
          - interfaces: *0
            fields:
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
                  name: Boolean
                  kind: SCALAR
                name: delete
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
                  name: user_collection_update
                  kind: INPUT_OBJECT
                name: update
              - type:
                  name: user_collection_insert
                  kind: INPUT_OBJECT
                name: insert
              type:
                ofType:
                  ofType:
                    ofType:
                      name: results___user_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_metainfo_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: order_collection
            kind: OBJECT
            name: results___Mutation
            description: generated from avro-schema for Mutation
          - kind: ENUM
            enumValues:
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
            name: __DirectiveLocation
            description: A Directive can be adjacent to many parts of the GraphQL language,
              a __DirectiveLocation describes one such possible adjacencies.
          - kind: ENUM
            enumValues:
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
            name: __TypeKind
            description: An enum describing what kind of type a given `__Type` is.
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: Double
                  kind: SCALAR
                kind: NON_NULL
              name: price
            - type:
                name: Boolean
                kind: SCALAR
              name: in_stock
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: user_id
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_id
            - type:
                ofType:
                  name: Float
                  kind: SCALAR
                kind: NON_NULL
              name: discount
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: description
            name: order_collection_insert
            description: generated from avro-schema for order_collection_insert
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __DirectiveLocation
                      kind: ENUM
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: locations
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __InputValue
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: args
            kind: OBJECT
            name: __Directive
            description: A Directive provides a way to describe alternate runtime execution
              and type validation behavior in a GraphQL document. In some cases, you need
              to provide options to alter GraphQL’s execution behavior in ways field arguments
              will not suffice, such as conditionally including or skipping a field. Directives
              provide this by describing additional information to the executor.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Int
                  kind: SCALAR
                kind: NON_NULL
              name: int
            kind: OBJECT
            name: Int_box
            description: Box (wrapper) around union variant
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: isDeprecated
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: deprecationReason
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __InputValue
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: args
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __Type
                  kind: OBJECT
                kind: NON_NULL
              name: type
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            kind: OBJECT
            name: __Field
            description: Object and Interface types are described by a list of Fields, each
              of which has a name, potentially a list of arguments, and a return type.
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___store___store___second_address___address
            description: generated from avro-schema for address
          - possibleTypes:
            - name: Int_box
              kind: OBJECT
            - name: String_box
              kind: OBJECT
            name: results___order_metainfo_collection___store___store___external_id___external_id
            kind: UNION
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___second_address___address
            description: generated from avro-schema for address
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: string
            kind: OBJECT
            name: String_box
            description: Box (wrapper) around union variant
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __Type
                  kind: OBJECT
                kind: NON_NULL
              name: type
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: defaultValue
              description: A GraphQL-formatted string representing the default value for this
                input value.
            kind: OBJECT
            name: __InputValue
            description: Arguments provided to Fields or Directives and the input fields of
              an InputObject are represented as Input Values which describe their type and
              optionally a default value.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: isDeprecated
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: deprecationReason
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            kind: OBJECT
            name: __EnumValue
            description: One possible value for a given Enum. Enum values are unique values,
              not a placeholder for a string or numeric value. However an Enum value is returned
              in a JSON response as a string.
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: state
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: zip
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: city
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: street
            name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: user_connection
            description: generated from the connection "user_connection" of collection "order_collection"
              using collection "user_collection"
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: string
            name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id___String_box
            description: Box (wrapper) around union variant
          - name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___parametrized_tags___InputMap
            kind: SCALAR
          - kind: SCALAR
            name: results___order_metainfo_collection___store___store___parametrized_tags___Map
            description: Map is a dictionary with string keys and values of arbitrary but
              same among all values type
          - kind: INPUT_OBJECT
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
            name: order_collection_pcre
            description: generated from avro-schema for order_collection_pcre
          - interfaces: *0
            fields:
            - isDeprecated: false
              args:
              - type:
                  name: user_collection_pcre
                  kind: INPUT_OBJECT
                name: pcre
              - type:
                  name: String
                  kind: SCALAR
                name: offset
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
                  name: Int
                  kind: SCALAR
                name: limit
              - type:
                  name: String
                  kind: SCALAR
                name: middle_name
              type:
                ofType:
                  ofType:
                    ofType:
                      name: results___user_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_metainfo_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: order_collection
            kind: OBJECT
            name: results___Query
            description: generated from avro-schema for Query
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                name: __Type
                kind: OBJECT
              name: mutationType
              description: If this server supports mutation, the type that mutation operations
                will be rooted at.
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __Type
                  kind: OBJECT
                kind: NON_NULL
              name: queryType
              description: The type that query operations will be rooted at.
            - isDeprecated: false
              args: *0
              type:
                name: __Type
                kind: OBJECT
              name: subscriptionType
              description: If this server supports subscriptions, the type that subscription
                operations will be rooted at.
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __Type
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: types
              description: A list of all types supported by this server.
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __Directive
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: directives
              description: A list of all directives supported by this server.
            kind: OBJECT
            name: __Schema
            description: A GraphQL Schema defines the capabilities of a GraphQL server. It
              exposes all available types and directives on the server, as well as the entry
              points for query and mutation operations.
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: user_id
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: last_name
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: first_name
            - type:
                name: String
                kind: SCALAR
              name: middle_name
            name: user_collection_insert
            description: generated from avro-schema for user_collection_insert
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: Int
                  kind: SCALAR
                kind: NON_NULL
              name: int
            name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id___Int_box
            description: Box (wrapper) around union variant
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: Int
                  kind: SCALAR
                kind: NON_NULL
              name: int
            name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id___Int_box
            description: Box (wrapper) around union variant
          - name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id
            kind: SCALAR
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: state
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: zip
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: city
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: street
            name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___second_address___address
            description: generated from avro-schema for address
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: state
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: zip
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: city
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: street
            kind: OBJECT
            name: results___order_metainfo_collection___store___store___second_address___address
            description: generated from avro-schema for address
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
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
                ofType:
                  ofType:
                    name: __EnumValue
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: enumValues
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    name: __InputValue
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: inputFields
            - isDeprecated: false
              args:
              - type:
                  name: Boolean
                  kind: SCALAR
                name: includeDeprecated
                defaultValue: 'false'
              type:
                ofType:
                  ofType:
                    name: __Field
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: fields
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __TypeKind
                  kind: ENUM
                kind: NON_NULL
              name: kind
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    name: __Type
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: interfaces
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    name: __Type
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: possibleTypes
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            kind: OBJECT
            name: __Type
            description: The fundamental unit of any GraphQL Schema is the type. There are
              many kinds of types in GraphQL as represented by the `__TypeKind` enum. Depending
              on the kind of a type, certain fields describe information about that type.
              Scalar types provide no information beyond a name and description, while Enum
              types provide their values. Object and Interface types provide the fields they
              describe. Abstract types, Union and Interface, provide the Object types possible
              at runtime. List and NonNull types compose other types.
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___address___address
                  kind: INPUT_OBJECT
                kind: NON_NULL
              name: address
            - type:
                ofType:
                  name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___second_address___address
                  kind: INPUT_OBJECT
                kind: NON_NULL
              name: second_address
            - type:
                ofType:
                  ofType:
                    ofType:
                      name: String
                      kind: SCALAR
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: tags
            - type:
                ofType:
                  name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___external_id___external_id
                  kind: SCALAR
                kind: NON_NULL
              name: external_id
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - type:
                ofType:
                  name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store___parametrized_tags___InputMap
                  kind: SCALAR
                kind: NON_NULL
              name: parametrized_tags
            name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store
            description: generated from avro-schema for store
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: state
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: zip
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: city
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: street
            kind: OBJECT
            name: results___order_metainfo_collection___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_metainfo_id
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_metainfo_id_copy
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: metainfo
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_id
            - type:
                ofType:
                  name: arguments___order_metainfo_collection___insert___order_metainfo_collection_insert___store___store
                  kind: INPUT_OBJECT
                kind: NON_NULL
              name: store
            name: order_metainfo_collection_insert
            description: generated from avro-schema for order_metainfo_collection_insert
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___second_address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: user_collection_pcre
            description: generated from avro-schema for user_collection_pcre
          - kind: INPUT_OBJECT
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
            name: order_metainfo_collection_pcre
            description: generated from avro-schema for order_metainfo_collection_pcre
          - kind: INPUT_OBJECT
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
            name: user_collection_update
            description: generated from avro-schema for user_collection_update
          - name: Double
            kind: SCALAR
          - kind: SCALAR
            name: Boolean
            description: The `Boolean` scalar type represents `true` or `false`.
          - kind: INPUT_OBJECT
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
                ofType:
                  ofType:
                    name: String
                    kind: SCALAR
                  kind: NON_NULL
                kind: LIST
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
            name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store
            description: generated from avro-schema for store
          - name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___parametrized_tags___InputMap
            kind: SCALAR
          - kind: SCALAR
            name: Int
            description: The `Int` scalar type represents non-fractional signed whole numeric
              values. Int can represent values between -(2^31) and 2^31 - 1.
          - name: Float
            kind: SCALAR
          - kind: INPUT_OBJECT
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
            name: order_metainfo_collection_update
            description: generated from avro-schema for order_metainfo_collection_update
          - kind: INPUT_OBJECT
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
            name: order_metainfo_connection
            description: generated from the connection "order_metainfo_connection" of collection
              "order_collection" using collection "order_metainfo_collection"
          - kind: INPUT_OBJECT
            inputFields:
            - type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: string
            name: arguments___order_metainfo_collection___update___order_metainfo_collection_update___store___store___external_id___external_id___String_box
            description: Box (wrapper) around union variant
          - kind: SCALAR
            name: String
            description: The `String` scalar type represents textual data, represented as
              UTF-8 character sequences. The String type is most often used by GraphQL to
              represent free-form human-readable text.
          - kind: INPUT_OBJECT
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
            name: order_collection_update
            description: generated from avro-schema for order_collection_update
          queryType:
            name: results___Query
          directives:
          - args:
            - type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: if
              description: Included when true.
            locations:
            - FIELD
            - FRAGMENT_SPREAD
            - INLINE_FRAGMENT
            name: include
            description: Directs the executor to include this field or fragment only when
              the `if` argument is true.
          - args:
            - type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: if
              description: Skipped when true.
            locations:
            - FIELD
            - FRAGMENT_SPREAD
            - INLINE_FRAGMENT
            name: skip
            description: Directs the executor to skip this field or fragment when the `if`
              argument is true.
    ]]):strip())
    -- luacheck: pop

    -- luacheck: push max line length 156
    local exp_result_avro_schema_2 = yaml.decode(([[
        __schema:
          mutationType:
            name: results___Mutation
          types:
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___store___store
            description: generated from avro-schema for store
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store
            description: generated from avro-schema for store
          - interfaces: &0 []
            fields:
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
                  name: user_connection
                  kind: INPUT_OBJECT
                name: user_connection
              - type:
                  name: String
                  kind: SCALAR
                name: user_id
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
                name: description
              type:
                ofType:
                  ofType:
                    ofType:
                      name: results___order_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: order_connection
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: first_name
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: user_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: last_name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: middle_name
            kind: OBJECT
            name: results___user_collection
            description: generated from avro-schema for user
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: description
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Double
                  kind: SCALAR
                kind: NON_NULL
              name: price
            - isDeprecated: false
              args:
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
                name: results___order_metainfo_collection
                kind: OBJECT
              name: order_metainfo_connection
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: in_stock
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: user_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Float
                  kind: SCALAR
                kind: NON_NULL
              name: discount
            - isDeprecated: false
              args:
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
              type:
                name: results___user_collection
                kind: OBJECT
              name: user_connection
            kind: OBJECT
            name: results___order_collection
            description: generated from avro-schema for order
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_metainfo_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_metainfo_id_copy
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: metainfo
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: order_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store
                  kind: OBJECT
                kind: NON_NULL
              name: store
            kind: OBJECT
            name: results___order_metainfo_collection
            description: generated from avro-schema for order_metainfo
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___address___address
                  kind: OBJECT
                kind: NON_NULL
              name: address
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___second_address___address
                  kind: OBJECT
                kind: NON_NULL
              name: second_address
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: String
                      kind: SCALAR
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: tags
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___external_id___external_id
                  kind: UNION
                kind: NON_NULL
              name: external_id
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: results___order_metainfo_collection___store___store___parametrized_tags___Map
                  kind: SCALAR
                kind: NON_NULL
              name: parametrized_tags
            kind: OBJECT
            name: results___order_metainfo_collection___store___store
            description: generated from avro-schema for store
          - interfaces: *0
            fields:
            - isDeprecated: false
              args:
              - type:
                  name: user_collection_pcre
                  kind: INPUT_OBJECT
                name: pcre
              - type:
                  name: String
                  kind: SCALAR
                name: offset
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
                  name: Int
                  kind: SCALAR
                name: limit
              - type:
                  name: String
                  kind: SCALAR
                name: middle_name
              type:
                ofType:
                  ofType:
                    ofType:
                      name: results___user_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_metainfo_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: order_collection
            kind: OBJECT
            name: results___Mutation
            description: generated from avro-schema for Mutation
          - kind: ENUM
            enumValues:
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
            name: __TypeKind
            description: An enum describing what kind of type a given `__Type` is.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __DirectiveLocation
                      kind: ENUM
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: locations
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __InputValue
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: args
            kind: OBJECT
            name: __Directive
            description: A Directive provides a way to describe alternate runtime execution
              and type validation behavior in a GraphQL document. In some cases, you need
              to provide options to alter GraphQL’s execution behavior in ways field arguments
              will not suffice, such as conditionally including or skipping a field. Directives
              provide this by describing additional information to the executor.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Int
                  kind: SCALAR
                kind: NON_NULL
              name: int
            kind: OBJECT
            name: Int_box
            description: Box (wrapper) around union variant
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___store___store___second_address___address
            description: generated from avro-schema for address
          - possibleTypes:
            - name: Int_box
              kind: OBJECT
            - name: String_box
              kind: OBJECT
            name: results___order_metainfo_collection___store___store___external_id___external_id
            kind: UNION
          - kind: INPUT_OBJECT
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
            name: order_metainfo_connection
            description: generated from the connection "order_metainfo_connection" of collection
              "order_collection" using collection "order_metainfo_collection"
          - name: Float
            kind: SCALAR
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: string
            kind: OBJECT
            name: String_box
            description: Box (wrapper) around union variant
          - kind: INPUT_OBJECT
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
            name: user_connection
            description: generated from the connection "user_connection" of collection "order_collection"
              using collection "user_collection"
          - kind: SCALAR
            name: results___order_metainfo_collection___store___store___parametrized_tags___Map
            description: Map is a dictionary with string keys and values of arbitrary but
              same among all values type
          - kind: INPUT_OBJECT
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
            name: order_collection_pcre
            description: generated from avro-schema for order_collection_pcre
          - interfaces: *0
            fields:
            - isDeprecated: false
              args:
              - type:
                  name: user_collection_pcre
                  kind: INPUT_OBJECT
                name: pcre
              - type:
                  name: String
                  kind: SCALAR
                name: offset
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
                  name: Int
                  kind: SCALAR
                name: limit
              - type:
                  name: String
                  kind: SCALAR
                name: middle_name
              type:
                ofType:
                  ofType:
                    ofType:
                      name: results___user_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_metainfo_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
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
                ofType:
                  ofType:
                    ofType:
                      name: results___order_collection
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: order_collection
            kind: OBJECT
            name: results___Query
            description: generated from avro-schema for Query
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                name: __Type
                kind: OBJECT
              name: mutationType
              description: If this server supports mutation, the type that mutation operations
                will be rooted at.
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __Type
                  kind: OBJECT
                kind: NON_NULL
              name: queryType
              description: The type that query operations will be rooted at.
            - isDeprecated: false
              args: *0
              type:
                name: __Type
                kind: OBJECT
              name: subscriptionType
              description: If this server supports subscriptions, the type that subscription
                operations will be rooted at.
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __Type
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: types
              description: A list of all types supported by this server.
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __Directive
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: directives
              description: A list of all directives supported by this server.
            kind: OBJECT
            name: __Schema
            description: A GraphQL Schema defines the capabilities of a GraphQL server. It
              exposes all available types and directives on the server, as well as the entry
              points for query and mutation operations.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: isDeprecated
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: deprecationReason
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            kind: OBJECT
            name: __EnumValue
            description: One possible value for a given Enum. Enum values are unique values,
              not a placeholder for a string or numeric value. However an Enum value is returned
              in a JSON response as a string.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
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
                ofType:
                  ofType:
                    name: __EnumValue
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: enumValues
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    name: __InputValue
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: inputFields
            - isDeprecated: false
              args:
              - type:
                  name: Boolean
                  kind: SCALAR
                name: includeDeprecated
                defaultValue: 'false'
              type:
                ofType:
                  ofType:
                    name: __Field
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: fields
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __TypeKind
                  kind: ENUM
                kind: NON_NULL
              name: kind
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    name: __Type
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: interfaces
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    name: __Type
                    kind: OBJECT
                  kind: NON_NULL
                kind: LIST
              name: possibleTypes
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            kind: OBJECT
            name: __Type
            description: The fundamental unit of any GraphQL Schema is the type. There are
              many kinds of types in GraphQL as represented by the `__TypeKind` enum. Depending
              on the kind of a type, certain fields describe information about that type.
              Scalar types provide no information beyond a name and description, while Enum
              types provide their values. Object and Interface types provide the fields they
              describe. Abstract types, Union and Interface, provide the Object types possible
              at runtime. List and NonNull types compose other types.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: state
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: zip
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: city
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: street
            kind: OBJECT
            name: results___order_metainfo_collection___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: user_collection_pcre
            description: generated from avro-schema for user_collection_pcre
          - name: Double
            kind: SCALAR
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __Type
                  kind: OBJECT
                kind: NON_NULL
              name: type
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: defaultValue
              description: A GraphQL-formatted string representing the default value for this
                input value.
            kind: OBJECT
            name: __InputValue
            description: Arguments provided to Fields or Directives and the input fields of
              an InputObject are represented as Input Values which describe their type and
              optionally a default value.
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: isDeprecated
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: deprecationReason
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  ofType:
                    ofType:
                      name: __InputValue
                      kind: OBJECT
                    kind: NON_NULL
                  kind: LIST
                kind: NON_NULL
              name: args
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: __Type
                  kind: OBJECT
                kind: NON_NULL
              name: type
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: name
            - isDeprecated: false
              args: *0
              type:
                name: String
                kind: SCALAR
              name: description
            kind: OBJECT
            name: __Field
            description: Object and Interface types are described by a list of Fields, each
              of which has a name, potentially a list of arguments, and a return type.
          - kind: SCALAR
            name: String
            description: The `String` scalar type represents textual data, represented as
              UTF-8 character sequences. The String type is most often used by GraphQL to
              represent free-form human-readable text.
          - kind: INPUT_OBJECT
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
            name: order_metainfo_collection_pcre
            description: generated from avro-schema for order_metainfo_collection_pcre
          - kind: SCALAR
            name: Int
            description: The `Int` scalar type represents non-fractional signed whole numeric
              values. Int can represent values between -(2^31) and 2^31 - 1.
          - kind: SCALAR
            name: Boolean
            description: The `Boolean` scalar type represents `true` or `false`.
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___address___address
            description: generated from avro-schema for address
          - kind: INPUT_OBJECT
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
            name: arguments___order_metainfo_collection___pcre___order_metainfo_collection_pcre___store___store___second_address___address
            description: generated from avro-schema for address
          - interfaces: *0
            fields:
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: state
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: zip
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: city
            - isDeprecated: false
              args: *0
              type:
                ofType:
                  name: String
                  kind: SCALAR
                kind: NON_NULL
              name: street
            kind: OBJECT
            name: results___order_metainfo_collection___store___store___second_address___address
            description: generated from avro-schema for address
          - kind: ENUM
            enumValues:
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
            name: __DirectiveLocation
            description: A Directive can be adjacent to many parts of the GraphQL language,
              a __DirectiveLocation describes one such possible adjacencies.
          queryType:
            name: results___Query
          directives:
          - args:
            - type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: if
              description: Included when true.
            locations:
            - FIELD
            - FRAGMENT_SPREAD
            - INLINE_FRAGMENT
            name: include
            description: Directs the executor to include this field or fragment only when
              the `if` argument is true.
          - args:
            - type:
                ofType:
                  name: Boolean
                  kind: SCALAR
                kind: NON_NULL
              name: if
              description: Skipped when true.
            locations:
            - FIELD
            - FRAGMENT_SPREAD
            - INLINE_FRAGMENT
            name: skip
            description: Directs the executor to skip this field or fragment when the `if`
              argument is true.
    ]]):strip())
    -- luacheck: pop

    -- note: we don't add mutation arguments for avro-schema-2*
    local exp_result = test_utils.major_avro_schema_version() == 3 and
        exp_result_avro_schema_3 or exp_result_avro_schema_2

    test_utils.show_trace(function()
        local gql_query = gql_wrapper:compile(query)
        local result = gql_query:execute({})
        test:is_deeply(result.data, exp_result, 'introspection query')
    end)

    assert(test:check(), 'check plan')
end

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

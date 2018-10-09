local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local multihead_conn_testdata = {}

function multihead_conn_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "hero": {
            "name": "hero",
            "type": "record",
            "fields": [
                { "name": "hero_id", "type": "string" },
                { "name": "hero_type", "type" : "string" },
                { "name": "banking_type", "type" : "string" }
            ]
        },
        "human": {
            "name": "human",
            "type": "record",
            "fields": [
                { "name": "hero_id", "type": "string" },
                { "name": "name", "type": "string" },
                { "name": "episode", "type": "string"},
                { "name": "address_id", "type": "string"}
            ]
        },
        "address": {
            "name": "address",
            "type": "record",
            "fields": [
                { "name": "address_id", "type": "string" },
                { "name": "street", "type": "string" },
                { "name": "city", "type": "string" },
                { "name": "state", "type": "string" },
                { "name": "zip", "type": "string" }
            ]
        },
        "hobby": {
            "name": "hobby",
            "type": "record",
            "fields": [
                { "name": "hobby_id", "type": "string" },
                { "name": "hero_id", "type": "string" },
                { "name": "summary", "type": "string" }
            ]
        },
        "starship": {
            "name": "starship",
            "type": "record",
            "fields": [
                { "name": "hero_id", "type": "string" },
                { "name": "model", "type": "string" },
                { "name": "episode", "type": "string"}
            ]
        },
        "credit_account": {
            "name": "credit_account",
            "type": "record",
            "fields": [
                { "name": "account_id", "type": "string" },
                { "name": "hero_id", "type": "string" },
                { "name": "bank_id", "type": "string" }
            ]
        },
        "bank": {
            "name": "bank",
            "type": "record",
            "fields": [
                { "name": "bank_id", "type": "string" },
                { "name": "name", "type": "string" }
            ]
        },
        "credit_operation": {
            "name": "credit_operation",
            "type": "record",
            "fields": [
                { "name": "credit_operation_id", "type": "string" },
                { "name": "account_id", "type": "string" },
                { "name": "type", "type": "string" },
                { "name": "amount", "type": "long" }
            ]
        },
        "dublon_account": {
            "name": "dublon_account",
            "type": "record",
            "fields": [
                { "name": "account_id", "type": "string" },
                { "name": "hero_id", "type": "string" }
            ]
        }
    }]])

    local collections = json.decode([[{
        "hero_collection": {
            "schema_name": "hero",
            "connections": [
                {
                    "name": "hero_connection",
                    "type": "1:1",
                    "variants": [
                        {
                            "determinant": {"hero_type": "human"},
                            "destination_collection": "human_collection",
                            "parts": [
                                {
                                    "source_field": "hero_id",
                                    "destination_field": "hero_id"
                                }
                            ],
                            "index_name": "human_id_index"
                        },
                        {
                            "determinant": {"hero_type": "starship"},
                            "destination_collection": "starship_collection",
                            "parts": [
                                {
                                    "source_field": "hero_id",
                                    "destination_field": "hero_id"
                                }
                            ],
                            "index_name": "starship_id_index"
                        }
                    ]
                },
                {
                    "name": "hero_banking_connection",
                    "type": "1:N",
                    "variants": [
                        {
                            "determinant": {"banking_type": "credit"},
                            "destination_collection": "credit_account_collection",
                            "parts": [
                                {
                                    "source_field": "hero_id",
                                    "destination_field": "hero_id"
                                }
                            ],
                            "index_name": "credit_hero_id_index"
                        },
                        {
                            "determinant": {"banking_type": "dublon"},
                            "destination_collection": "dublon_account_collection",
                            "parts": [
                                {
                                    "source_field": "hero_id",
                                    "destination_field": "hero_id"
                                }
                            ],
                            "index_name": "dublon_hero_id_index"
                        }
                    ]
                }
            ]
        },
        "human_collection": {
            "schema_name": "human",
            "connections": [
                {
                    "name": "address_connection",
                    "type": "1:1",
                    "destination_collection": "address_collection",
                    "parts": [
                        {
                            "source_field": "address_id",
                            "destination_field": "address_id"
                        }
                    ],
                    "index_name": "address_id_index"
                },
                {
                    "name": "hobby_connection",
                    "type": "1:N",
                    "destination_collection": "hobby_collection",
                    "parts": [
                        {
                            "source_field": "hero_id",
                            "destination_field": "hero_id"
                        }
                    ],
                    "index_name": "hero_id_index"
                }
            ]
        },
        "address_collection": {
            "schema_name": "address",
            "connections": []
        },
        "hobby_collection": {
            "schema_name": "hobby",
            "connections": []
        },
        "starship_collection": {
            "schema_name": "starship",
            "connections": []
        },
        "credit_account_collection": {
            "schema_name": "credit_account",
            "connections": [
                {
                    "name": "bank_connection",
                    "type": "1:1",
                    "destination_collection": "bank_collection",
                    "parts": [
                        {
                            "source_field": "bank_id",
                            "destination_field": "bank_id"
                        }
                    ],
                    "index_name": "bank_id_index"
                },
                {
                    "name": "credit_operation_connection",
                    "type": "1:N",
                    "destination_collection": "credit_operation_collection",
                    "parts": [
                        {
                            "source_field": "account_id",
                            "destination_field": "account_id"
                        }
                    ],
                    "index_name": "account_id_index"
                }
            ]
        },
        "bank_collection": {
            "schema_name": "bank",
            "connections": []
        },
        "credit_operation_collection": {
            "schema_name": "credit_operation",
            "connections": []
        },
        "dublon_account_collection": {
            "schema_name": "dublon_account",
            "connections": []
        }
    }]])

    local service_fields = {
        hero = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        human = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        address = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        hobby = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        starship = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        credit_account = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        bank = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        credit_operation = {
            { name = 'expires_on', type = 'long', default = 0 },
        },
        dublon_account = {
            { name = 'expires_on', type = 'long', default = 0 },
        }
    }

    local indexes = {
        hero_collection = {
            hero_id_index = {
                service_fields = {},
                fields = { 'hero_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },

        human_collection = {
            human_id_index = {
                service_fields = {},
                fields = { 'hero_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },

        address_collection = {
            address_id_index = {
                service_fields = {},
                fields = { 'address_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            }
        },

        hobby_collection = {
            hobby_id_index = {
                service_fields = {},
                fields = { 'hobby_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            hero_id_index = {
                service_fields = {},
                fields = { 'hero_id' },
                index_type = 'tree',
                unique = false,
                primary = false,
            },
        },

        starship_collection = {
            starship_id_index = {
                service_fields = {},
                fields = { 'hero_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },

        credit_account_collection = {
            credit_id_index = {
                service_fields = {},
                fields = { 'account_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            credit_hero_id_index = {
                service_fields = {},
                fields = { 'hero_id' },
                index_type = 'tree',
                unique = false,
                primary = false,
            }
        },

        bank_collection = {
            bank_id_index = {
                service_fields = {},
                fields = { 'bank_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            }
        },

        credit_operation_collection = {
            credit_operation_id_index = {
                service_fields = {},
                fields = { 'credit_operation_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            account_id_index = {
                service_fields = {},
                fields = { 'account_id' },
                index_type = 'tree',
                unique = false,
                primary = false,
            }
        },

        dublon_account_collection = {
            dublon_id_index = {
                service_fields = {},
                fields = { 'account_id' },
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            dublon_hero_id_index = {
                service_fields = {},
                fields = { 'hero_id' },
                index_type = 'tree',
                unique = false,
                primary = false,
            }
        }
    }

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function multihead_conn_testdata.init_spaces()
    local ID_FIELD_NUM = 2
    local HERO_ID_FIELD_NUM = 3
    local ACCOUNT_ID_FIELD_NUM = 3

    box.once('test_space_init_spaces', function()
        box.schema.create_space('hero_collection')
        box.space.hero_collection:create_index('hero_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('human_collection')
        box.space.human_collection:create_index('human_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('address_collection')
        box.space.address_collection:create_index('address_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('hobby_collection')
        box.space.hobby_collection:create_index('hobby_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )
        box.space.hobby_collection:create_index('hero_id_index',
            { type = 'tree', unique = false,
              parts = { HERO_ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('starship_collection')
        box.space.starship_collection:create_index('starship_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('credit_account_collection')
        box.space.credit_account_collection:create_index('credit_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )
        box.space.credit_account_collection:create_index('credit_hero_id_index',
            { type = 'tree', unique = false,
              parts = { HERO_ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('bank_collection')
        box.space.bank_collection:create_index('bank_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('credit_operation_collection')
        box.space.credit_operation_collection:create_index(
            'credit_operation_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )
        box.space.credit_operation_collection:create_index('account_id_index',
            { type = 'tree', unique = false,
              parts = { ACCOUNT_ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('dublon_account_collection')
        box.space.dublon_account_collection:create_index('dublon_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )
        box.space.dublon_account_collection:create_index('dublon_hero_id_index',
            { type = 'tree', unique = false,
              parts = { HERO_ID_FIELD_NUM, 'string' }}
        )
    end)
end

function multihead_conn_testdata.fill_test_data(shard)
    local shard = shard or box.space

    shard.hero_collection:replace(
        { 1827767717, 'hero_id_1', 'human', 'credit'})
    shard.hero_collection:replace(
        { 1827767717, 'hero_id_2', 'starship', 'dublon'})
    shard.hero_collection:replace(
        { 1827767717, 'hero_id_3', 'human', 'credit'})
    shard.hero_collection:replace(
        { 1827767717, 'hero_id_4', 'starship', 'dublon'})

    shard.human_collection:replace(
        { 1827767717, 'hero_id_1', 'Luke', "EMPR", 'address_id_1'})
    shard.human_collection:replace(
        { 1827767717, 'hero_id_3', 'Luke_2', "EMPR", 'address_id_2'})

    shard.address_collection:replace(
        { 1827767717, 'address_id_1', 'street 1', 'city 1', 'state 1', 'zip 1'})
    shard.address_collection:replace(
        { 1827767717, 'address_id_2', 'street 2', 'city 2', 'state 2', 'zip 2'})

    shard.hobby_collection:replace(
        { 1827767717, 'hobby_id_1', 'hero_id_1', 'skating'})
    shard.hobby_collection:replace(
        { 1827767717, 'hobby_id_2', 'hero_id_1', 'diving'})
    shard.hobby_collection:replace(
        { 1827767717, 'hobby_id_3', 'hero_id_1', 'drawing'})

    shard.starship_collection:replace(
        { 1827767717, 'hero_id_2', 'Falcon-42', "NEW"})
    shard.starship_collection:replace(
        { 1827767717, 'hero_id_4', 'Falcon-42_2', "NEW"})

    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_1', 'hero_id_1', 'bank_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_2', 'hero_id_1', 'bank_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_3', 'hero_id_1', 'bank_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_4', 'hero_id_3', 'bank_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_5', 'hero_id_3', 'bank_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_6', 'hero_id_3', 'bank_id_1'})

    shard.bank_collection:replace(
        { 1827767717, 'bank_id_1', 'bank 1' })

    shard.credit_operation_collection:replace(
        { 1827767717, 'op_1', 'credit_account_id_1', 'charge', 5000 })
    shard.credit_operation_collection:replace(
        { 1827767717, 'op_2', 'credit_account_id_1', 'refund', 5000 })

    shard.dublon_account_collection:replace(
        { 1827767717, 'dublon_account_id_1', 'hero_id_2'})
    shard.dublon_account_collection:replace(
        { 1827767717, 'dublon_account_id_2', 'hero_id_2'})
    shard.dublon_account_collection:replace(
        { 1827767717, 'dublon_account_id_3', 'hero_id_2'})
    shard.dublon_account_collection:replace(
        { 1827767717, 'dublon_account_id_4', 'hero_id_4'})
    shard.dublon_account_collection:replace(
        { 1827767717, 'dublon_account_id_5', 'hero_id_4'})
    shard.dublon_account_collection:replace(
        { 1827767717, 'dublon_account_id_6', 'hero_id_4'})
end

function multihead_conn_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.human_collection:drop()
    box.space.address_collection:drop()
    box.space.hobby_collection:drop()
    box.space.starship_collection:drop()
    box.space.hero_collection:drop()
    box.space.credit_account_collection:drop()
    box.space.bank_collection:drop()
    box.space.credit_operation_collection:drop()
    box.space.dublon_account_collection:drop()
end

function multihead_conn_testdata.run_queries(gql_wrapper)
    local test = tap.test('multihead_conn')
    test:plan(4)

    -- note on hero_banking_connection:
    -- As credit_account_collection has [credit_account] GraphQL List type
    -- it should be wrapped into box type
    -- credit_account_collections is a box type (GraphQL Object) with
    -- one field credit_account_collection (which has GraphQL List type)
    local query = [[
        query obtainHeroes($hero_id: String) {
            hero_collection(hero_id: $hero_id) {
                hero_id
                hero_type
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
                banking_type
                hero_banking_connection {
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

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query)
    end)

    local variables_1_1 = {hero_id = 'hero_id_1'}
    local result_1_1 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_1)
    end)
    local exp_result_1_1 = yaml.decode(([[
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
    ]]):strip())
    test:is_deeply(result_1_1.data, exp_result_1_1, '1_1')

    local variables_1_2 = {hero_id = 'hero_id_2'}
    local result_1_2 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_2)
    end)
    local exp_result_1_2 = yaml.decode(([[
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
                  hero_id: hero_id_2
                - account_id: dublon_account_id_2
                  hero_id: hero_id_2
                - account_id: dublon_account_id_3
                  hero_id: hero_id_2
    ]]):strip())
    test:is_deeply(result_1_2.data, exp_result_1_2, '1_2')

    -- Ensure BFS executor does not fail in case of list of objects from
    -- different collections.
    --
    -- [1]: https://github.com/tarantool/graphql/issues/245
    local variables_1_3 = {}
    local result_1_3 = test_utils.show_trace(function()
        return gql_query_1:execute(variables_1_3)
    end)
    local exp_result_1_3 = yaml.decode(([[
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
        - hero_id: hero_id_2
          hero_type: starship
          hero_connection:
            starship_collection:
                model: Falcon-42
          banking_type: dublon
          hero_banking_connection:
            dublon_account_collection:
                - account_id: dublon_account_id_1
                  hero_id: hero_id_2
                - account_id: dublon_account_id_2
                  hero_id: hero_id_2
                - account_id: dublon_account_id_3
                  hero_id: hero_id_2
        - hero_id: hero_id_3
          hero_type: human
          hero_connection:
            human_collection:
                name: Luke_2
          banking_type: credit
          hero_banking_connection:
            credit_account_collection:
                - account_id: credit_account_id_4
                  hero_id: hero_id_3
                - account_id: credit_account_id_5
                  hero_id: hero_id_3
                - account_id: credit_account_id_6
                  hero_id: hero_id_3
        - hero_id: hero_id_4
          hero_type: starship
          hero_connection:
            starship_collection:
                model: Falcon-42_2
          banking_type: dublon
          hero_banking_connection:
            dublon_account_collection:
                - account_id: dublon_account_id_4
                  hero_id: hero_id_4
                - account_id: dublon_account_id_5
                  hero_id: hero_id_4
                - account_id: dublon_account_id_6
                  hero_id: hero_id_4
    ]]):strip())
    test:is_deeply(result_1_3.data, exp_result_1_3, '1_3')

    -- Ensure BFS executor does not fail in case of a connection inside a
    -- multihead connection.
    --
    -- We test all four combinations of 1:1 / 1:N multihead connections and
    -- 1:1 / 1:N nested regular connections.
    --
    -- [1]: https://github.com/tarantool/graphql/issues/245

    local query_2 = [[
        {
            hero_collection(hero_id: "hero_id_1") {
                hero_id
                hero_connection {
                    ... on box_human_collection {
                        human_collection {
                            name
                            address_connection {
                                street
                                city
                                state
                                zip
                            }
                            hobby_connection {
                                summary
                            }
                        }
                    }
                }
                hero_banking_connection {
                    ... on box_array_credit_account_collection {
                        credit_account_collection {
                            account_id
                            hero_id
                            bank_connection {
                                name
                            }
                            credit_operation_connection {
                                type
                                amount
                            }
                        }
                    }
                }
            }
        }
    ]]

    local gql_query_2 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_2)
    end)

    local exp_result_2 = yaml.decode(([[
        hero_collection:
        - hero_id: hero_id_1
          hero_connection:
            human_collection:
              name: Luke
              address_connection:
                street: street 1
                city: city 1
                state: state 1
                zip: zip 1
              hobby_connection:
                - summary: skating
                - summary: diving
                - summary: drawing
          hero_banking_connection:
            credit_account_collection:
              - account_id: credit_account_id_1
                hero_id: hero_id_1
                bank_connection:
                  name: bank 1
                credit_operation_connection:
                  - type: charge
                    amount: 5000
                  - type: refund
                    amount: 5000
              - account_id: credit_account_id_2
                hero_id: hero_id_1
                bank_connection:
                  name: bank 1
                credit_operation_connection: []
              - account_id: credit_account_id_3
                hero_id: hero_id_1
                bank_connection:
                  name: bank 1
                credit_operation_connection: []
    ]]):strip())

    local result_2 = test_utils.show_trace(function()
        return gql_query_2:execute({})
    end)

    test:is_deeply(result_2.data, exp_result_2, '2')

    assert(test:check(), 'check plan')
end

return multihead_conn_testdata

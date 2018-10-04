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
                { "name": "episode", "type": "string"}
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
                { "name": "hero_id", "type": "string" }
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
            "connections": []
        },
        "starship_collection": {
            "schema_name": "starship",
            "connections": []
        },
        "credit_account_collection": {
            "schema_name": "credit_account",
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
        starship = {
            { name = 'expires_on', type = 'long', default = 0 },
        },

        credit_account = {
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

    box.once('test_space_init_spaces', function()
        box.schema.create_space('hero_collection')
        box.space.hero_collection:create_index('hero_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
        )

        box.schema.create_space('human_collection')
        box.space.human_collection:create_index('human_id_index',
            { type = 'tree', unique = true, parts = { ID_FIELD_NUM, 'string' }}
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
        { 1827767717, 'hero_id_1', 'Luke', "EMPR"})
    shard.human_collection:replace(
        { 1827767717, 'hero_id_3', 'Luke_2', "EMPR"})

    shard.starship_collection:replace(
        { 1827767717, 'hero_id_2', 'Falcon-42', "NEW"})
    shard.starship_collection:replace(
        { 1827767717, 'hero_id_4', 'Falcon-42_2', "NEW"})

    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_1', 'hero_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_2', 'hero_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_3', 'hero_id_1'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_4', 'hero_id_3'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_5', 'hero_id_3'})
    shard.credit_account_collection:replace(
        { 1827767717, 'credit_account_id_6', 'hero_id_3'})

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
    box.space.starship_collection:drop()
    box.space.hero_collection:drop()
    box.space.credit_account_collection:drop()
    box.space.dublon_account_collection:drop()
end

function multihead_conn_testdata.run_queries(gql_wrapper)
    local test = tap.test('multihead_conn')
    test:plan(3)

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

    assert(test:check(), 'check plan')
end

return multihead_conn_testdata

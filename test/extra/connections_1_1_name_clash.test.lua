#!/usr/bin/env tarantool
local fio = require('fio')
local json = require('json')
local test = require('tap').test('connections 1:1 name clash')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local graphql = require('graphql')

box.cfg{ wal_mode="none" }
test:plan(3)

local schemas = json.decode([[{
    "hero": {
        "name": "hero",
        "type": "record",
        "fields": [
            { "name": "hero_id", "type": "string" },
            { "name": "hero_type", "type" : "string" }
        ]
    },
    "human": {
        "name": "human",
        "type": "record",
        "fields": [
            { "name": "hero_id", "type": "string" }
        ]
    },
    "starship": {
        "name": "starship",
        "type": "record",
        "fields": [
            { "name": "hero_id", "type": "string" }
        ]
    },
    "hero_info": {
        "name": "hero_info",
        "type": "record",
        "fields": [
            { "name": "hero_id", "type": "string" }
        ]
    },
    "hero_meta": {
        "name": "hero_meta",
        "type": "record",
        "fields": [
            { "name": "hero_id", "type": "string" }
        ]
    }
}]])

-- In the following tests we use different collections with different
-- potential name clash cases. We check that such combinations do not cause
-- name clash errors.

-- Potential clash in the second connection ("hero_info_collection" connection).
-- Connection name has the same name as it's destination_collection.
local collections_1 = json.decode([[{
    "hero_collection": {
        "schema_name": "hero",
        "connections": [
            {
                "name": "hero_connection",
                "type": "1:1",
                "variants": [
                    {
                        "determinant": { "hero_type": "human" },
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
                        "determinant": { "hero_type": "starship" },
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
                "name" : "hero_info_collection",
                "type": "1:1",
                "destination_collection": "hero_info_collection",
                "parts": [
                    { "source_field": "hero_id", "destination_field": "hero_id" }
                ],
                "index_name": "hero_id_index"
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
    "hero_info_collection": {
        "schema_name": "hero_info",
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
    hero_info = {
        { name = 'expires_on', type = 'long', default = 0 },
    },
    hero_meta = {
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
    hero_info_collection = {
        hero_id_index = {
            service_fields = {},
            fields = { 'hero_id' },
            index_type = 'tree',
            unique = true,
            primary = true
        }
    },
    hero_meta_collection = {
        hero_id_index = {
            service_fields = {},
            fields = { 'hero_id' },
            index_type = 'tree',
            unique = true,
            primary = true
        }
    }
}

local gql_wrapper_1 = graphql.new({
    schemas = schemas,
    collections = collections_1,
    service_fields = service_fields,
    indexes = indexes,
    accessor = 'space'
})

test:isnt(gql_wrapper_1, nil)

-- Potential clash between first ("hero_connection") and second
-- ("human_collection") connections:
-- "human_collection" connection has the same name as destination_collection of
-- "hero_connection" first variant.
local collections_2 = json.decode([[{
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
                "name" : "human_collection",
                "type": "1:1",
                "destination_collection": "hero_info_collection",
                "parts": [
                    { "source_field": "hero_id", "destination_field": "hero_id" }
                ],
                "index_name": "hero_id_index"
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
    "hero_info_collection": {
        "schema_name": "hero_info",
        "connections": []
    }
}]])

local gql_wrapper_2 = graphql.new({
    schemas = schemas,
    collections = collections_2,
    service_fields = service_fields,
    indexes = indexes,
    accessor = 'space'
})

test:isnt(gql_wrapper_2, nil)

-- Potential clash between second ("hero_meta_collection") and third
-- ("hero_metainfo_connection") connections:
-- "hero_meta_connection" has the same name as destination_collection of
-- "hero_metainfo_connection".
local collections_3 = json.decode([[{
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
                "name": "hero_meta_collection",
                "type": "1:1",
                "destination_collection": "hero_info_collection",
                "parts": [
                    { "source_field": "hero_id", "destination_field": "hero_id" }
                ],
                "index_name": "hero_id_index"
            },
            {
                "name": "hero_metainfo_connection",
                "type": "1:1",
                "destination_collection": "hero_meta_collection",
                "parts": [
                    { "source_field": "hero_id", "destination_field": "hero_id" }
                ],
                "index_name": "hero_id_index"
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
    "hero_info_collection": {
        "schema_name": "hero_info",
        "connections": []
    },
    "hero_meta_collection": {
        "schema_name": "hero_meta",
        "connections": []
    }
}]])

local gql_wrapper_3 = graphql.new({
    schemas = schemas,
    collections = collections_3,
    service_fields = service_fields,
    indexes = indexes,
    accessor = 'space'
})
test:isnt(gql_wrapper_3, nil)

test:check()

os.exit()

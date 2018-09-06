#!/usr/bin/env tarantool

-- https://github.com/tarantool/graphql/issues/170

local fio = require('fio')
local json = require('json')
local tap = require('tap')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local graphql = require('graphql')
local utils = require('graphql.utils')
local test_utils = require('test.test_utils')

local test = tap.test('connections do not hide fields')

box.cfg{wal_mode='none'}
test:plan(3)

local schemas = json.decode([[{
    "foo": {
        "name": "foo",
        "type": "record",
        "fields": [
            { "name": "foo_id", "type": "string" },
            { "name": "bar", "type": "string" }
        ]
    },
    "bar": {
        "name": "bar",
        "type": "record",
        "fields": [
            { "name": "bar_id", "type": "string" }
        ]
    }
}]])

local collections_1 = json.decode([[{
    "foo": {
        "schema_name": "foo",
        "connections": [
            {
                "type": "1:1",
                "name": "bar",
                "destination_collection": "bar",
                "parts": [
                   {
                       "source_field": "bar",
                       "destination_field": "bar_id"
                   }
                ],
                "index_name": "bar_id"
            }
        ]
    },
    "bar": {
        "schema_name": "bar",
        "connections": []
    }
}]])

local collections_2 = table.deepcopy(collections_1)
collections_2.foo.connections[1].type = '1:N'

local collections_3 = table.deepcopy(collections_1)
collections_3.foo.connections[1].name = 'bar_c'
table.insert(collections_3.foo.connections, collections_3.foo.connections[1])

local service_fields = {
    foo = {},
    bar = {},
}

local indexes = {
    foo = {
        foo_id = {
            service_fields = {},
            fields = {'foo_id'},
            index_type = 'tree',
            unique = true,
            primary = true,
        },
    },
    bar = {
        bar_id = {
            service_fields = {},
            fields = {'bar_id'},
            index_type = 'tree',
            unique = true,
            primary = true,
        },
    },
}

local cfg_1 = utils.merge_tables({
    schemas = schemas,
    collections = collections_1,
    service_fields = service_fields,
    indexes = indexes,
    accessor = 'space',
}, test_utils.test_conf_graphql_opts())

local cfg_2 = table.deepcopy(cfg_1)
cfg_2.collections = collections_2

local cfg_3 = table.deepcopy(cfg_1)
cfg_3.collections = collections_3

-- schema field name clash with 1:1 connection name
-- ------------------------------------------------

local exp_err = '[collection "foo"] the connection "bar" is named ' ..
    'as a schema field'

local ok, err = pcall(graphql.new, cfg_1)

test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
    'do not hide fields for 1:1 connections')

-- schema field name clash with 1:N connection name
-- ------------------------------------------------

local exp_err = '[collection "foo"] the connection "bar" is named ' ..
    'as a schema field'

local ok, err = pcall(graphql.new, cfg_2)

test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
    'do not hide fields for 1:N connections')

-- connections names clash
-- -----------------------

local exp_err = '[collection "foo"] two connections are named "bar_c"'

local ok, err = pcall(graphql.new, cfg_3)

test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
    'do not hide a connection with a connection')

os.exit(test:check() == true and 0 or 1)

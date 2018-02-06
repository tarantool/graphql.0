#!/usr/bin/env tarantool

-- init shard, fill spaces
-- -----------------------

env = require('test_run')
test_run = env.new()

shard = require('shard')

-- we need at least four servers to make sure we have several (two) servers
-- within each replica set and several (two) replica sets

test_run:cmd("setopt delimiter ';'")
SERVERS = {'shard1', 'shard2', 'shard3', 'shard4'};
init_shard(SERVERS, {
    servers = {
        { uri = instance_uri('1'), zone = '0' },
        { uri = instance_uri('2'), zone = '1' },
        { uri = instance_uri('3'), zone = '2' },
        { uri = instance_uri('4'), zone = '3' },
    },
    login = 'guest',
    password = '',
    redundancy = 2,
}, 'shard_redundancy');
test_run:cmd("setopt delimiter ''");

fill_test_data()

-- graphql queries
-- ---------------

fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)"):gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

json = require('json')
yaml = require('yaml')
graphql = require('graphql')

metadata = get_test_metadata()
schemas = metadata.schemas
collections = metadata.collections
service_fields = metadata.service_fields
indexes = metadata.indexes

test_run:cmd("setopt delimiter ';'")

accessor = graphql.accessor_shard.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
});

gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
});

-- query all orders of an user
-- ---------------------------

-- That is needed to make sure a select on a shard with redundancy will skip
-- duplicates of an one tuple from different servers within a replica set.

query_user_order = [[
    query user_order($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            last_name
            first_name
            order_connection {
                order_id
                description
            }
        }
    }
]];

gql_query_user_order = gql_wrapper:compile(query_user_order);
variables_user_order = {user_id = 'user_id_42'};
gql_query_user_order:execute(variables_user_order);

-- stop shards
-- -----------

test_run:drop_cluster(SERVERS)

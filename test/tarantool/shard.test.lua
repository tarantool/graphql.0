#!/usr/bin/env tarantool

-- init shard, fill spaces
-- -----------------------

env = require('test_run')
test_run = env.new()

shard = require('shard')

test_run:cmd("setopt delimiter ';'")
SERVERS = {'shard1', 'shard2'};
init_shard(SERVERS, {
    servers = {
        { uri = instance_uri('1'), zone = '0' },
        { uri = instance_uri('2'), zone = '1' },
    },
    login = 'guest',
    password = '',
    redundancy = 1,
});
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

query_1 = [[
    query user_by_order($order_id: String) {
        order_collection(order_id: $order_id) {
            order_id
            description
            user_connection {
                user_id
                last_name
                first_name
            }
        }
    }
]];

variables_1 = {order_id = 'order_id_1'};
gql_query_1 = gql_wrapper:compile(query_1);
gql_query_1:execute(variables_1);

query_2 = [[
    query user_order($user_id: String, $first_name: String, $limit: Int,
            $offset: Long) {
        user_collection(user_id: $user_id, first_name: $first_name) {
            user_id
            last_name
            first_name
            order_connection(limit: $limit, offset: $offset) {
                order_id
                description
            }
        }
    }
]];

gql_query_2 = gql_wrapper:compile(query_2);

variables_2 = {user_id = 'user_id_1'};
gql_query_2:execute(variables_2);

variables_2_2 = {user_id = 'user_id_42', limit = 10, offset = 10};
gql_query_2:execute(variables_2_2);

variables_2_3 = {user_id = 'user_id_42', limit = 10, offset = 38};
gql_query_2:execute(variables_2_3);

-- XXX: it triggers segfault on shard, waiting for fix
-- https://github.com/tarantool/tarantool/issues/3101
--variables_2_4 = {
--    first_name = 'first name 42',
--    limit = 3,
--    offset = 39,
--};
--gql_query_2:execute(variables_2_4);

-- XXX: it triggers *flaky* segfault on shard, waiting for fix
-- https://github.com/tarantool/tarantool/issues/3101
--query_3 = [[
--    query users($limit: Int, $offset: Long) {
--        user_collection(limit: $limit, offset: $offset) {
--            user_id
--            last_name
--            first_name
--        }
--    }
--]];
--variables_3 = {limit = 10, offset = 50};
--gql_query_3 = gql_wrapper:compile(query_3);
--gql_query_3:execute(variables_3);

-- extra filter for 1:N connection
-- -------------------------------

query_4 = [[
    query user_order($first_name: String, $description: String) {
        user_collection(first_name: $first_name) {
            user_id
            last_name
            first_name
            order_connection(description: $description) {
                order_id
                description
            }
        }
    }
]];

gql_query_4 = gql_wrapper:compile(query_4);

-- XXX: it triggers segfault on shard, waiting for fix
-- https://github.com/tarantool/tarantool/issues/3101
-- should match 1 order
--variables_4_1 = {
--    first_name = 'Ivan',
--    description = 'first order of Ivan',
--};
--gql_query_4:execute(variables_4_1);

-- XXX: it triggers segfault on shard, waiting for fix
-- https://github.com/tarantool/tarantool/issues/3101
-- should match no orders
--variables_4_2 = {
--    first_name = 'Ivan',
--    description = 'non-existent order',
--};
--gql_query_4:execute(variables_4_2);

-- extra filter for 1:1 connection;
-- -------------------------------;

query_5 = [[
    query user_by_order($first_name: String, $description: String) {
        order_collection(description: $description) {
            order_id
            description
            user_connection(first_name: $first_name) {
                user_id
                last_name
                first_name
            }
        }
    }
]];

gql_query_5 = gql_wrapper:compile(query_5);

-- should match 1 user;
-- XXX: it triggers segfault on shard, waiting for fix
-- https://github.com/tarantool/tarantool/issues/3101
--variables_5_1 = {
--    first_name = 'Ivan',
--    description = 'first order of Ivan',
--};
--gql_query_5:execute(variables_5_1);

-- should match no users (or give an error?);
--variables_5_2 = {
--    first_name = 'non-existent user',
--    description = 'first order of Ivan',
--};
--gql_query_5:execute(variables_5_2);

test_run:cmd("setopt delimiter ''");

-- stop shards
-- -----------

test_run:drop_cluster(SERVERS)

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

fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)"):gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

graphql = require('graphql')
testdata = require('test.testdata.compound_index_testdata')

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
test_run:cmd('switch shard1')
require('test.testdata.compound_index_testdata').init_spaces()
test_run:cmd('switch shard2')
require('test.testdata.compound_index_testdata').init_spaces()
test_run:cmd('switch shard3')
require('test.testdata.compound_index_testdata').init_spaces()
test_run:cmd('switch shard4')
require('test.testdata.compound_index_testdata').init_spaces()
test_run:cmd('switch default')

-- upload test data
testdata.fill_test_data(shard)

-- acquire metadata
metadata = testdata.get_test_metadata()
schemas = metadata.schemas
collections = metadata.collections
service_fields = metadata.service_fields
indexes = metadata.indexes

-- build accessor and graphql schemas
-- ----------------------------------

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
test_run:cmd("setopt delimiter ''");

testdata.run_queries(gql_wrapper)

-- clean up
-- --------

test_run:cmd('switch shard1')
require('test.testdata.compound_index_testdata').drop_spaces()
test_run:cmd('switch shard2')
require('test.testdata.compound_index_testdata').drop_spaces()
test_run:cmd('switch shard3')
require('test.testdata.compound_index_testdata').drop_spaces()
test_run:cmd('switch shard4')
require('test.testdata.compound_index_testdata').drop_spaces()
test_run:cmd('switch default')

test_run:drop_cluster(SERVERS)

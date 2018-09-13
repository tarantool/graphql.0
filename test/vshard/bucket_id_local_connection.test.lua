#!/usr/bin/env tarantool
local yaml = require('yaml')
local test_run = require('test_run').new()
local tap = require('tap')
local vshard = require('vshard')
local graphql = require('graphql')
local fio = require('fio')
local testdata = require('test.vshard.vshard_testdata')
local test_utils = require('test.test_utils')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local vutils = require('test.vshard.vshard_utils')
local vb = require('test.virtual_box')
local test = tap.test('vshard')
if test_utils.major_avro_schema_version() == 2 then
    test:plan(2)
else
    test:plan(8)
end

local SERVERS = {'shard1', 'shard2', 'shard3', 'shard4'}
test_run:create_cluster(SERVERS, 'vshard')
vutils.cluster_setup()
vutils.cluster_eval(testdata.init_spaces)

local router = vshard.router.new('my_router', vutils.cfg)
router:bootstrap()
local virtbox = vb.get_virtbox_for_accessor('vshard', {
    meta = testdata.meta,
    router = router,
})
testdata.fill_data(virtbox)

local gql = graphql.new({
    schemas = testdata.meta.schemas,
    collections = testdata.meta.collections,
    indexes = testdata.meta.indexes,
    service_fields = testdata.meta.service_fields,
    accessor = 'vshard',
    accessor_funcs = nil,
    vshard = testdata.meta.vshard,
    router = router,
})
assert(type(gql) == 'table')

local query_user = [[
    query user($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            first_name
        }
    }
]]
local compiled_query = gql.compile(gql, query_user)
local result = compiled_query:execute({user_id = 'user_id_1'})
test:is_deeply(result.data.user_collection,
    {{user_id='user_id_1' ,first_name= 'Ivan'}})

local query_user_and_orders = [[
    query user($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            first_name
            order_connection {
                user_id
                order_id
            }
        }
    }
]]
local compiled_query = gql.compile(gql, query_user_and_orders)
local result = compiled_query:execute({user_id = 'user_id_1'})
test:is_deeply(result.data.user_collection,
    yaml.decode([[
      - user_id: user_id_1
        first_name: Ivan
        order_connection:
        - user_id: user_id_1
          order_id: order_id_1
        - user_id: user_id_1
          order_id: order_id_2
    ]]))

if test_utils.major_avro_schema_version() == 2 then
    assert(test:check(), 'check plan')
    os.exit(0)
end

local query_user_full_scan = [[
    query user($last_name: String) {
        user_collection(last_name: $last_name) {
            user_id
            first_name
            order_connection {
                user_id
                order_id
            }
        }
    }
]]
local compiled_query = gql.compile(gql, query_user_full_scan)
local _ = compiled_query:execute({last_name = 'Ivanov'})

local mutation_insert_user = [[
    mutation insert_user {
        user_collection(insert: {
            user_id: "user_id_new_1"
            first_name: "Peter"
            last_name: "Petrov"
            middle_name: "MiddleName"
        }) {
            user_id
            first_name
            last_name
            bucket_id
        }
    }
]]
local compiled_query = gql.compile(gql, mutation_insert_user)
local _ = compiled_query:execute({})

-- Insert into table, for which the bucket_id cannot be calculated.
local mutation_insert_into_adjective_table = [[
    mutation insert_order_meta($order_meta: order_metainfo_collection_insert) {
        order_metainfo_collection(insert: $order_meta) {
            order_id
            metainfo
        }
    }
]]

local function get_orger_meta_for_order(i)
    return {
        metainfo = 'order metainfo ' .. i,
        order_metainfo_id = 'order_metainfo_id_' .. i,
        order_metainfo_id_copy = 'order_metainfo_id_' .. i,
        order_id = 'order_id_' .. i,
        store = {
            name = 'store ' .. i,
            address = {
                street = 'street ' .. i,
                city = 'city ' .. i,
                state = 'state ' .. i,
                zip = 'zip ' .. i,
            },
            second_address = {
                street = 'second street ' .. i,
                city = 'second city ' .. i,
                state = 'second state ' .. i,
                zip = 'second zip ' .. i,
            },
            external_id = i % 2 == 1 and {int = i} or
                {string = 'eid_' .. i},
            tags = {'fast', 'new'},
            parametrized_tags = {
                size = 'medium',
                since = '2018-01-01'
            },
        }
    }
end

local order_meta = get_orger_meta_for_order(1)
local compiled_query = gql.compile(gql, mutation_insert_into_adjective_table)
local result = compiled_query:execute({order_meta = order_meta})
test:like(result.errors[1].message, 'Cannot find or infer bucket id for object',
    'Valid error is raised in case bucket_id cannot be inferred')
order_meta.bucket_id =
    virtbox.user_collection:get_object({'user_id_1'}).bucket_id
local result = compiled_query:execute({order_meta = order_meta})
test:is_deeply(result.data.order_metainfo_collection,
    {{metainfo = 'order metainfo 1', order_id = 'order_id_1'}},
    'Insert successful in case the bucket_id is provided in input')

-- Insert into table, fetching bucket_id from parent.
-- Order lookup performs fullscan.
local mutation_insert_into_child_table = [[
    mutation insert_order_meta($order_id: String, $order_meta: order_metainfo_collection_insert) {
        order_collection(order_id: $order_id) {
            order_id
            order_metainfo_connection(insert: $order_meta) {
                order_id
                metainfo
            }
        }
    }
]]
local compiled_query = gql.compile(gql, mutation_insert_into_child_table)
for i = 2, 3 do
    local meta = get_orger_meta_for_order(i)
    local _ = compiled_query:execute({
        order_id = "order_id_1",
        meta = meta,
    })
end

local mutation_update_user = [[
    mutation update_user {
        user_collection(user_id: "user_id_1", update: {
            first_name: "Peter"
            last_name: "Petrov"
        }) {
            user_id
            first_name
            last_name
        }
    }
]]
local compiled_query = gql.compile(gql, mutation_update_user)
local result = compiled_query:execute({})
test:is_deeply(result.data.user_collection,
    yaml.decode([[
    - user_id: user_id_1
      last_name: Petrov
      first_name: Peter
    ]]), 'The object was updated')

local mutation_update_bucket_id = [[
    mutation update_user {
        user_collection(user_id: "user_id_1", update: {
            bucket_id: 2000
        }) {
            user_id
            bucket_id
        }
    }
]]
local compiled_query = gql.compile(gql, mutation_update_bucket_id)
local result = compiled_query:execute({})
test:like(result.errors[1].message,
    "Attempt to modify a tuple field 'bucket_id'",
    "bucket_id field is not allowed to be modified")

local mutation_delete_user = [[
    mutation delete_user {
        user_collection(user_id: "user_id_1", delete: true) {
            user_id
            first_name
            last_name
        }
    }
]]
local compiled_query = gql.compile(gql, mutation_delete_user)
assert(virtbox.user_collection:get({'user_id_1'}) ~= nil,
    'User exists before the delete')
local result = compiled_query:execute({})
print(yaml.encode(result))
test:is_deeply(result.data.user_collection,
    yaml.decode([[
    - user_id: user_id_1
      last_name: Petrov
      first_name: Peter
    ]]), 'The object which was deleted')
test:is(nil, virtbox.user_collection:get({'user_id_1'}),
    'User was really deleted')

test_run:drop_cluster(SERVERS)
assert(test:check(), 'check plan')

os.exit(0)

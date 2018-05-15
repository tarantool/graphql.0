#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local utils = require('graphql.utils')
local test_utils = require('test.utils')
local testdata = require('test.testdata.common_testdata')

box.cfg({})

local function get_tuple(virtbox, collection_name, key)
    if virtbox[collection_name].get ~= nil then
        return virtbox[collection_name]:get(key)
    end

    local tuples = virtbox:secondary_select(collection_name, 0, nil, key)
    return tuples[1]
end

local function delete_tuple(virtbox, collection_name, key)
    if virtbox[collection_name].get ~= nil then
        return virtbox[collection_name]:delete(key)
    end

    for _, zone in ipairs(virtbox.shards) do
        for _, node in ipairs(zone) do
            virtbox:space_call(collection_name, node, function(space_obj)
                space_obj:delete(key)
            end)
        end
    end
end

local function check_insert(test, gql_wrapper, virtbox, mutation_insert,
        exp_result_insert, opts)
    local opts = opts or {}
    local dont_pass_variables = opts.dont_pass_variables or false
    local meta = opts.meta

    utils.show_trace(function()
        test:plan(7)
        local user_id = 'user_id_new_1'
        local order_id = 'order_id_new_1'
        local variables_insert = {
            user = {
                user_id = user_id,
                first_name = 'Peter',
                last_name = 'Petrov',
            },
            order = {
                order_id = order_id,
                user_id = user_id,
                description = 'Peter\'s order',
                price = 0.0,
                discount = 0.0,
                -- in_stock = true, -- we'll check it set from default value
                -- from the schema
            }
        }
        local gql_mutation_insert = gql_wrapper:compile(mutation_insert)
        -- check mutation result from graphql
        local result = gql_mutation_insert:execute(dont_pass_variables and {} or
            variables_insert)
        test:is_deeply(result, exp_result_insert, 'insert result')
        -- check inserted user
        local tuple = get_tuple(virtbox, 'user_collection', {user_id})
        test:ok(tuple ~= nil, 'tuple was inserted')
        local exp_tuple = test_utils.flatten_object(virtbox, meta,
            'user_collection', variables_insert.user, {0})
        test:is_deeply(tuple:totable(), exp_tuple, 'inserted tuple is correct')
        -- check inserted order
        local tuple = get_tuple(virtbox, 'order_collection', {order_id})
        test:ok(tuple ~= nil, 'tuple was inserted')
        local exp_tuple = test_utils.flatten_object(virtbox, meta,
            'order_collection', variables_insert.order)
        test:is_deeply(tuple:totable(), exp_tuple, 'inserted tuple is correct')
        -- clean up inserted tuples & check
        delete_tuple(virtbox, 'user_collection', {user_id})
        local tuple = get_tuple(virtbox, 'user_collection', {user_id})
        test:ok(tuple == nil, 'tuple was deleted')
        delete_tuple(virtbox, 'order_collection', {order_id})
        local tuple = get_tuple(virtbox, 'order_collection', {order_id})
        test:ok(tuple == nil, 'tuple was deleted')
        assert(test:check(), 'check plan')
    end)
end

local function run_queries(gql_wrapper, virtbox, meta)
    local test = tap.test('mutation')
    test:plan(6)

    -- {{{ insert

    local mutation_insert_1 = [[
        mutation insert_user_and_order($user: user_collection_insert,
                $order: order_collection_insert) {
            user_collection(insert: $user) {
                user_id
                first_name
                last_name
            }
            order_collection(insert: $order) {
                order_id
                description
                in_stock
            }
        }
    ]]

    local exp_result_insert_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_new_1
          first_name: Peter
          last_name: Petrov
        order_collection:
        - order_id: order_id_new_1
          description: Peter's order
          in_stock: true
    ]]):strip())

    check_insert(test:test('insert_1'), gql_wrapper, virtbox, mutation_insert_1,
        exp_result_insert_1, {meta = meta})

    -- the same with immediate argument
    local mutation_insert_1i = [[
        mutation insert_user_and_order {
            user_collection(insert: {
                user_id: "user_id_new_1"
                first_name: "Peter"
                last_name: "Petrov"
            }) {
                user_id
                first_name
                last_name
            }
            order_collection(insert: {
                order_id: "order_id_new_1"
                user_id: "user_id_new_1"
                description: "Peter's order"
                price: 0.0
                discount: 0.0
                # in_stock: true should be set as default value
            }) {
                order_id
                description
                in_stock
            }
        }
    ]]

    check_insert(test:test('insert_1i'), gql_wrapper, virtbox,
        mutation_insert_1i, exp_result_insert_1, {meta = meta,
        dont_pass_variables = true})

    -- test "insert" argument is forbidden in a non-top level field
    local mutation_insert_2 = [[
        query insert_user($order_id: String, $user: user_collection_insert) {
            order_collection(order_id: $order_id) {
                order_id
                user_connection(insert: $user) {
                    user_id
                    first_name
                    last_name
                }
            }
        }
    ]]
    local ok, err = pcall(gql_wrapper.compile, gql_wrapper, mutation_insert_2)
    local err_exp = 'Non-existent argument "insert"'
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        '"insert" argument is forbidden in a non-top level field')

    -- test "insert" argument is forbidden in a query
    local query_insert = [[
        query insert_user($user: user_collection_insert) {
            user_collection(insert: $user) {
                user_id
                first_name
                last_name
            }
        }
    ]]
    local ok, err = pcall(gql_wrapper.compile, gql_wrapper, query_insert)
    local err_exp = 'Non-existent argument "insert"'
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        '"insert" argument is forbidden in a query')

    -- test "insert" argument is forbidden with object arguments
    local mutation_insert_3i = [[
        mutation insert_user_and_order {
            user_collection(user_id: "some_id", insert: {
                user_id: "user_id_new_1"
                first_name: "Peter"
                last_name: "Petrov"
            }) {
                user_id
                first_name
                last_name
            }
        }
    ]]
    local gql_mutation_insert_3i = gql_wrapper:compile(mutation_insert_3i)
    local ok, err = pcall(gql_mutation_insert_3i.execute,
        gql_mutation_insert_3i, {})
    local err_exp = '"insert" must be the only argument when it is present'
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        '"insert" argument is forbidden with other filters (object arguments)')

    -- test "insert" argument is forbidden with list arguments
    local mutation_insert_4i = [[
        mutation insert_user_and_order {
            user_collection(limit: 1, insert: {
                user_id: "user_id_new_1"
                first_name: "Peter"
                last_name: "Petrov"
            }) {
                user_id
                first_name
                last_name
            }
        }
    ]]
    local gql_mutation_insert_4i = gql_wrapper:compile(mutation_insert_4i)
    local ok, err = pcall(gql_mutation_insert_4i.execute,
        gql_mutation_insert_4i, {})
    local err_exp = '"insert" must be the only argument when it is present'
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        '"insert" argument is forbidden with other filters (list arguments)')

    -- XXX: test inserting an object into a collection with subrecords

    -- }}}

    -- {{{ inner level inserts: disabled
    --[=[

    -- We disabled these inserts for now. It is planned to support
    -- insert-by-a-connection with some different argument name to avoid any
    -- confusion.

    local mutation_insert_2 = [[
        mutation insert_user_and_order($user: user_collection_insert,
                $order: order_collection_insert) {
            order_collection(insert: $order) {
                order_id
                description
                user_connection(insert: $user) {
                    user_id
                    first_name
                    last_name
                }
            }
        }
    ]]

    local exp_result_insert_2 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_new_1
          description: Peter's order
          user_connection:
            user_id: user_id_new_1
            first_name: Peter
            last_name: Petrov
    ]]):strip())

    check_insert(test:test('insert_2'), gql_wrapper, virtbox, mutation_insert_2,
        exp_result_insert_2)

    local mutation_insert_3 = [[
        mutation insert_user_and_order($user: user_collection_insert,
                $order: order_collection_insert) {
            user_collection(insert: $user) {
                user_id
                last_name
                first_name
                order_connection(insert: $order) {
                    order_id
                    description
                }
            }
        }
    ]]

    local exp_result_insert_3 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_new_1
          first_name: Peter
          last_name: Petrov
          order_connection:
          - order_id: order_id_new_1
            description: Peter's order
    ]]):strip())

    check_insert(test:test('insert_3'), gql_wrapper, virtbox,mutation_insert_3,
        exp_result_insert_3)

    ]=]--
    -- }}}

    assert(test:check(), 'check plan')
end

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

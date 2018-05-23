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

local function replace_tuple(virtbox, collection_name, key, tuple)
    if virtbox[collection_name].get ~= nil then
        return virtbox[collection_name]:replace(tuple)
    end

    if get_tuple(virtbox, collection_name, key) == nil then
        virtbox[collection_name]:insert(tuple)
    else
        for _, zone in ipairs(virtbox.shards) do
            for _, node in ipairs(zone) do
                virtbox:space_call(collection_name, node, function(space_obj)
                    if space_obj:get(key) ~= nil then
                        space_obj:replace(tuple)
                    end
                end)
            end
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
        local exp_tuple = test_utils.flatten_object(meta,
            'user_collection', variables_insert.user, {0})
        test:is_deeply(tuple:totable(), exp_tuple, 'inserted tuple is correct')
        -- check inserted order
        local tuple = get_tuple(virtbox, 'order_collection', {order_id})
        test:ok(tuple ~= nil, 'tuple was inserted')
        local exp_tuple = test_utils.flatten_object(meta,
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

local function check_update(test, gql_wrapper, virtbox, mutation_update,
        exp_result_update, opts)
    local opts = opts or {}
    local dont_pass_variables = opts.dont_pass_variables or false
    local meta = opts.meta
    local extra_xorder = opts.extra_xorder or {}
    local extra_xuser = opts.extra_xuser or {}

    utils.show_trace(function()
        test:plan(7)
        local user_id = 'user_id_1'
        local order_id = 'order_id_1'
        local variables_update = {
            user_id = user_id,
            order_id = order_id,
            xuser = {
                first_name = 'Peter',
                last_name = 'Petrov',
            },
            xorder = {
                description = 'Peter\'s order',
                price = 0.0,
                discount = 0.0,
                in_stock = false,
            }
        }
        for k, v in pairs(extra_xorder) do
            variables_update.xorder[k] = v
        end
        for k, v in pairs(extra_xuser) do
            variables_update.xuser[k] = v
        end

        -- save original objects
        local orig_user_tuple = get_tuple(virtbox, 'user_collection', {user_id})
        local orig_order_tuple = get_tuple(virtbox, 'order_collection',
            {order_id})
        local orig_user_object = test_utils.unflatten_tuple(meta,
            'user_collection', orig_user_tuple)
        local orig_order_object = test_utils.unflatten_tuple(meta,
            'order_collection', orig_order_tuple)

        local gql_mutation_update = gql_wrapper:compile(mutation_update)

        -- check mutation result from graphql
        local result = gql_mutation_update:execute(dont_pass_variables and {} or
            variables_update)
        test:is_deeply(result, exp_result_update, 'update result')
        -- check updated user
        local tuple = get_tuple(virtbox, 'user_collection', {user_id})
        test:ok(tuple ~= nil, 'updated tuple exists')
        local exp_object = table.copy(variables_update.xuser)
        for k, v in pairs(orig_user_object) do
            if exp_object[k] == nil then
                exp_object[k] = v
            end
        end
        local exp_tuple = test_utils.flatten_object(meta,
            'user_collection', exp_object, {1827767717})
        test:is_deeply(tuple:totable(), exp_tuple, 'updated tuple is correct')

        -- check updated order
        local tuple = get_tuple(virtbox, 'order_collection', {order_id})
        test:ok(tuple ~= nil, 'updated tuple exists')
        local exp_object = table.copy(variables_update.xorder)
        for k, v in pairs(orig_order_object) do
            if exp_object[k] == nil then
                exp_object[k] = v
            end
        end
        local exp_tuple = test_utils.flatten_object(meta,
            'order_collection', exp_object)
        test:is_deeply(tuple:totable(), exp_tuple, 'updated tuple is correct')

        -- replace back updated tuples & check
        replace_tuple(virtbox, 'user_collection', {user_id},
            orig_user_tuple)
        local tuple = get_tuple(virtbox, 'user_collection', {user_id})
        test:is_deeply(tuple:totable(), orig_user_tuple:totable(),
            'updated tuple was replaced back')
        replace_tuple(virtbox, 'order_collection', {order_id},
            orig_order_tuple)
        local tuple = get_tuple(virtbox, 'order_collection', {order_id})
        test:is_deeply(tuple:totable(), orig_order_tuple:totable(),
            'updated tuple was replaced back')
        assert(test:check(), 'check plan')
    end)
end

local function run_queries(gql_wrapper, virtbox, meta)
    local test = tap.test('mutation')
    test:plan(15)

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

    -- the same with an immediate argument
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

    -- {{{ update

    -- check on top level
    local mutation_update_1 = [[
        mutation update_user_and_order(
            $user_id: String,
            $order_id: String,
            $xuser: user_collection_update,
            $xorder: order_collection_update
        ) {
            user_collection(user_id: $user_id, update: $xuser) {
                user_id
                first_name
                last_name
            }
            order_collection(order_id: $order_id, update: $xorder) {
                order_id
                description
                in_stock
            }
        }
    ]]

    local exp_result_update_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          first_name: Peter
          last_name: Petrov
        order_collection:
        - order_id: order_id_1
          description: Peter's order
          in_stock: false
    ]]):strip())

    check_update(test:test('update_1'), gql_wrapper, virtbox, mutation_update_1,
        exp_result_update_1, {meta = meta})

    -- the same with an immediate argument
    local mutation_update_1i = [[
        mutation update_user_and_order {
            user_collection(user_id: "user_id_1", update: {
                first_name: "Peter"
                last_name: "Petrov"
            }) {
                user_id
                first_name
                last_name
            }
            order_collection(order_id: "order_id_1", update: {
                description: "Peter's order"
                price: 0.0
                discount: 0.0
                in_stock: false
            }) {
                order_id
                description
                in_stock
            }
        }
    ]]
    check_update(test:test('update_1i'), gql_wrapper, virtbox,
        mutation_update_1i, exp_result_update_1, {meta = meta,
        dont_pass_variables = true})

    -- check on non-top level field (update an object we read by a connection)

    local mutation_update_2 = [[
        mutation update_user_and_order(
            $user_id: String
            $order_id: String
            $xuser: user_collection_update
            $xorder: order_collection_update
        ) {
            # update nested user
            order_collection(order_id: $order_id) {
                order_id
                description
                user_connection(update: $xuser) {
                    user_id
                    first_name
                    last_name
                }
            }
            # update nested order
            user_collection(user_id: $user_id) {
                user_id
                first_name
                last_name
                order_connection(limit: 1, update: $xorder) {
                    order_id
                    description
                    in_stock
                }
            }
        }
    ]]

    local exp_result_update_2 = yaml.decode(([[
        ---
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          user_connection:
            user_id: user_id_1
            first_name: Peter
            last_name: Petrov
        user_collection:
        - user_id: user_id_1
          first_name: Peter
          last_name: Petrov
          order_connection:
          - order_id: order_id_1
            description: Peter's order
            in_stock: false
    ]]):strip())

    check_update(test:test('update_2'), gql_wrapper, virtbox, mutation_update_2,
        exp_result_update_2, {meta = meta})

    -- the same with different order of top-level fields

    local mutation_update_2r = [[
        mutation update_user_and_order(
            $user_id: String
            $order_id: String
            $xuser: user_collection_update
            $xorder: order_collection_update
        ) {
            # update nested order
            user_collection(user_id: $user_id) {
                user_id
                first_name
                last_name
                order_connection(limit: 1, update: $xorder) {
                    order_id
                    description
                    in_stock
                }
            }
            # update nested user
            order_collection(order_id: $order_id) {
                order_id
                description
                user_connection(update: $xuser) {
                    user_id
                    first_name
                    last_name
                }
            }
        }
    ]]

    local exp_result_update_2r = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          first_name: Ivan
          last_name: Ivanov
          order_connection:
          - order_id: order_id_1
            description: Peter's order
            in_stock: false
        order_collection:
        - order_id: order_id_1
          description: Peter's order
          user_connection:
            user_id: user_id_1
            first_name: Peter
            last_name: Petrov
    ]]):strip())

    check_update(test:test('update_2r'), gql_wrapper, virtbox,
        mutation_update_2r, exp_result_update_2r, {meta = meta})

    -- check with connection argument & connection field

    -- This case checks that filtering by connected fields is performed before
    -- update, but connected objects in the result given after the update.

    local mutation_update_3 = [[
        mutation update_user_and_order(
            $user_id: String,
            $order_id: String,
            $xuser: user_collection_update,
            $xorder: order_collection_update
        ) {
            user_collection(user_id: $user_id, update: $xuser) {
                user_id
                first_name
                last_name
            }
            order_collection(
                order_id: $order_id
                update: $xorder
                user_connection: {user_id: $user_id}
            ) {
                order_id
                description
                in_stock
                user_connection {
                    user_id
                }
            }
        }
    ]]

    local exp_result_update_3 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          first_name: Peter
          last_name: Petrov
        order_collection:
        - order_id: order_id_1
          description: Peter's order
          in_stock: false
          user_connection:
            user_id: 'user_id_2'
    ]]):strip())

    check_update(test:test('update_3'), gql_wrapper, virtbox, mutation_update_3,
        exp_result_update_3, {meta = meta, extra_xorder =
        {user_id = 'user_id_2'}})

    -- test "update" argument is forbidden in a query
    local query_update = [[
        query update_user_and_order(
            $user_id: String,
            $order_id: String,
            $xuser: user_collection_update,
            $xorder: order_collection_update
        ) {
            user_collection(user_id: $user_id, update: $xuser) {
                user_id
                first_name
                last_name
            }
            order_collection(order_id: $order_id, update: $xorder) {
                order_id
                description
                in_stock
            }
        }
    ]]
    local ok, err = pcall(gql_wrapper.compile, gql_wrapper, query_update)
    local err_exp = 'Non-existent argument "update"'
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        '"update" argument is forbidden in a query')

    -- test updating of a field by which a shard key is calculated (it is the
    -- first field: tuple[1])
    test:test('update 1st tuple field', function(test)
        test:plan(5)
        local mutation_update = [[
            mutation update_order_metainfo {
                order_metainfo_collection(
                    order_metainfo_id: "order_metainfo_id_1"
                    update: {
                        metainfo: "changed"
                    }
                ) {
                    metainfo
                    order_metainfo_id
                    order_id
                }
            }
        ]]
        local exp_result_update = yaml.decode(([[
            ---
            order_metainfo_collection:
            - metainfo: changed
              order_metainfo_id: order_metainfo_id_1
              order_id: order_id_1
        ]]):strip())

        -- check the original tuple
        local order_metainfo_id = 'order_metainfo_id_1'
        local orig_tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        local exp_orig_tuple =
            {'order metainfo 1', order_metainfo_id, 'order_id_1'}
        test:is_deeply(orig_tuple:totable(), exp_orig_tuple,
            'original tuple is the one that expected')

        -- check mutation result
        local gql_mutation_update = gql_wrapper:compile(mutation_update)
        local result = gql_mutation_update:execute({})
        test:is_deeply(result, exp_result_update, 'update result')

        -- check updated tuple
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:ok(tuple ~= nil, 'updated tuple exists')
        local exp_tuple =
            {'changed', order_metainfo_id, 'order_id_1'}
        test:is_deeply(tuple:totable(), exp_tuple, 'updated tuple is correct')

        -- replace back updated tuples & check
        replace_tuple(virtbox, 'order_metainfo_collection', {order_metainfo_id},
            orig_tuple)
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:is_deeply(tuple:totable(), orig_tuple:totable(),
            'updated tuple was replaced back')
    end)

    -- Test updating of a field of a primary key when:
    -- 1. it is NOT shard key field (tuple[1]);
    -- 2. it is the shard key field.
    -- Expected an error.
    local mutation_update_4 = [[
        mutation update_user(
            $user_id: String
            $xuser: user_collection_update
        ) {
            user_collection(user_id: $user_id, update: $xuser) {
                user_id
                first_name
                last_name
            }
        }
    ]]
    local gql_mutation_update_4 = gql_wrapper:compile(mutation_update_4)
    local variables_update_4 = {
        user_id = 'user_id_1',
        xuser = {
            user_id = 'user_id_201',
        }
    }
    local ok, err = pcall(gql_mutation_update_4.execute, gql_mutation_update_4,
        variables_update_4)
    local err_exp = "Attempt to modify a tuple field which is part of index " ..
        "'user_id_index' in space 'user_collection'"
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        'updating of a field of a primary key when it is NOT shard key field')

    local mutation_update_5 = [[
        mutation update_order(
            $order_id: String
            $xorder: order_collection_update
        ) {
            order_collection(order_id: $order_id, update: $xorder) {
                order_id
                description
            }
        }
    ]]
    local gql_mutation_update_5 = gql_wrapper:compile(mutation_update_5)
    local variables_update_5 = {
        order_id = 'order_id_1',
        xorder = {
            order_id = 'order_id_4001',
        }
    }
    local ok, err = pcall(gql_mutation_update_5.execute, gql_mutation_update_5,
        variables_update_5)
    local err_exp = "Attempt to modify a tuple field which is part of index " ..
        "'order_id_index' in space 'order_collection'"
    test:is_deeply({ok, test_utils.strip_error(err)}, {false, err_exp},
        'updating of a field of a primary key when it is shard key field')

    -- XXX: test updating an object in a collection with subrecords

    -- }}}

    -- {{{ XXX: delete
    -- }}}

    assert(test:check(), 'check plan')
end

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

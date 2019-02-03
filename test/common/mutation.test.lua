#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local utils = require('graphql.utils')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')
local test_run = utils.optional_require('test_run')
test_run = test_run and test_run.new()

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
        delete_tuple(virtbox, collection_name, key)
        virtbox[collection_name]:insert(tuple)
    end
end

-- replace back tuples & check
local function replace_user_and_order_back(test, virtbox, user_id, order_id,
        orig_user_tuple, orig_order_tuple)
    replace_tuple(virtbox, 'user_collection', {user_id}, orig_user_tuple)
    local tuple = get_tuple(virtbox, 'user_collection', {user_id})
    test:is_deeply(tuple:totable(), orig_user_tuple:totable(),
        'tuple was replaced back')
    replace_tuple(virtbox, 'order_collection', {order_id}, orig_order_tuple)
    local tuple = get_tuple(virtbox, 'order_collection', {order_id})
    test:is_deeply(tuple:totable(), orig_order_tuple:totable(),
        'tuple was replaced back')
end

local function check_insert(test, gql_wrapper, virtbox, mutation_insert,
        exp_result_insert, opts)
    local opts = opts or {}
    local dont_pass_variables = opts.dont_pass_variables or false
    local meta = opts.meta

    test_utils.show_trace(function()
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
        test:is_deeply(result.data, exp_result_insert, 'insert result')
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

local function check_insert_order_metainfo(test, gql_wrapper, virtbox,
        mutation_insert, exp_result_insert, opts)
    local opts = opts or {}
    local variables = opts.variables or {}

    test_utils.show_trace(function()
        test:plan(5)

        local order_metainfo_id = 'order_metainfo_id_4000'

        -- check the tuple was not inserted before
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:ok(tuple == nil, 'tuple was not inserted before')

        -- check mutation result
        local gql_mutation_insert = gql_wrapper:compile(mutation_insert)
        local result = gql_mutation_insert:execute(variables)
        test:is_deeply(result.data, exp_result_insert, 'insert result')

        -- check inserted tuple
        local EXTERNAL_ID_STRING = 1 -- 0 is for int
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:ok(tuple ~= nil, 'inserted tuple exists')
        local exp_tuple = {
            'order metainfo 4000',
            'order_metainfo_id_4000',
            'order_metainfo_id_4000',
            'order_id_4000',
            'store 4000',
            'street 4000',
            'city 4000',
            'state 4000',
            'zip 4000',
            'second street 4000',
            'second city 4000',
            'second state 4000',
            'second zip 4000',
            EXTERNAL_ID_STRING,
            'eid_4000',
            {'slow'},
            {size = 'small'},
        }
        test:is_deeply(tuple:totable(), exp_tuple, 'inserted tuple is correct')

        -- delete inserted tuple & check
        delete_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:ok(tuple == nil, 'inserted tuple was deleted')
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

    test_utils.show_trace(function()
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
        test:is_deeply(result.data, exp_result_update, 'update result')
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
        replace_user_and_order_back(test, virtbox, user_id, order_id,
            orig_user_tuple, orig_order_tuple)

        assert(test:check(), 'check plan')
    end)
end

local function check_update_order_metainfo(test, gql_wrapper, virtbox,
        mutation_update, exp_result_update, opts)
    local opts = opts or {}
    local variables = opts.variables or {}

    test_utils.show_trace(function()
        test:plan(5)

        -- check the original tuple
        local EXTERNAL_ID_INT = 0
        local EXTERNAL_ID_STRING = 1
        local order_metainfo_id = 'order_metainfo_id_1'
        local orig_tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        local exp_orig_tuple = {
            'order metainfo 1', order_metainfo_id, order_metainfo_id,
            'order_id_1', 'store 1', 'street 1', 'city 1', 'state 1',
            'zip 1', 'second street 1', 'second city 1', 'second state 1',
            'second zip 1', EXTERNAL_ID_INT, 1, {'fast', 'new'}, {
                size = 'medium',
                since = '2018-01-01',
            },
        }
        test:is_deeply(orig_tuple:totable(), exp_orig_tuple,
            'original tuple is the one that expected')

        -- check mutation result
        local gql_mutation_update = gql_wrapper:compile(mutation_update)
        local result = gql_mutation_update:execute(variables)
        test:is_deeply(result.data, exp_result_update, 'update result')

        -- check updated tuple
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:ok(tuple ~= nil, 'updated tuple exists')
        local exp_tuple = table.copy(exp_orig_tuple)
        exp_tuple[1] = 'changed'
        exp_tuple[7] = 'changed city'
        exp_tuple[11] = 'second changed city'
        exp_tuple[14] = EXTERNAL_ID_STRING
        exp_tuple[15] = 'eid changed'
        exp_tuple[16] = {'slow'}
        exp_tuple[17] = {size = 'small'}
        test:is_deeply(tuple:totable(), exp_tuple, 'updated tuple is correct')

        -- replace back updated tuples & check
        replace_tuple(virtbox, 'order_metainfo_collection', {order_metainfo_id},
            orig_tuple)
        local tuple = get_tuple(virtbox, 'order_metainfo_collection',
            {order_metainfo_id})
        test:is_deeply(tuple:totable(), orig_tuple:totable(),
            'updated tuple was replaced back')

        assert(test:check(), 'check plan')
    end)
end

local function check_delete(test, gql_wrapper, virtbox, mutation_delete,
        exp_result_delete, opts)
    local opts = opts or {}
    local dont_pass_variables = opts.dont_pass_variables or false

    test_utils.show_trace(function()
        test:plan(5)
        local user_id = 'user_id_1'
        local order_id = 'order_id_1'
        local variables_delete = {
            user_id = user_id,
            order_id = order_id,
        }

        -- save original tuples
        local orig_user_tuple = get_tuple(virtbox, 'user_collection', {user_id})
        local orig_order_tuple = get_tuple(virtbox, 'order_collection',
            {order_id})

        local gql_mutation_delete = gql_wrapper:compile(mutation_delete)

        -- check mutation result from graphql
        local result = gql_mutation_delete:execute(dont_pass_variables and {} or
            variables_delete)
        test:is_deeply(result.data, exp_result_delete, 'delete result')

        -- check the user was deleted
        local tuple = get_tuple(virtbox, 'user_collection', {user_id})
        test:ok(tuple == nil, 'tuple was deleted')

        -- check the order was deleted
        local tuple = get_tuple(virtbox, 'order_collection', {order_id})
        test:ok(tuple == nil, 'tuple was deleted')

        -- replace back deleted tuples & check
        replace_user_and_order_back(test, virtbox, user_id, order_id,
            orig_user_tuple, orig_order_tuple)

        assert(test:check(), 'check plan')
    end)
end

local function extract_storage_error(err)
    return err:gsub('^failed to execute operation on[^:]+: *', '')
end

local function run_queries(gql_wrapper, virtbox, meta)
    local test = tap.test('mutation')
    test:plan(23)

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
    local exp_err = 'Non-existent argument "insert"'
    test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
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
    local exp_err = 'Non-existent argument "insert"'
    test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
        '"insert" argument is forbidden in a query')

    -- test "insert" argument is forbidden with object arguments
    local mutation_insert_3i = [[
        mutation insert_user {
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
    local result = gql_mutation_insert_3i:execute({})
    local err = result.errors[1].message
    local exp_err = '"insert" must be the only argument when it is present'
    test:is(err, exp_err,
        '"insert" argument is forbidden with other filters (object arguments)')

    -- test "insert" argument is forbidden with list arguments
    local mutation_insert_4i = [[
        mutation insert_user {
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
    local result = gql_mutation_insert_4i:execute({})
    local err = result.errors[1].message
    local exp_err = '"insert" must be the only argument when it is present'
    test:is(err, exp_err,
        '"insert" argument is forbidden with other filters (list arguments)')

    -- test "insert" argument is forbidden with other extra argument
    local mutation_insert_5i = [[
        mutation insert_user {
            user_collection(delete: true, insert: {
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
    local gql_mutation_insert_5i = gql_wrapper:compile(mutation_insert_5i)
    local result = gql_mutation_insert_5i:execute({})
    local err = result.errors[1].message
    local exp_err = '"insert" must be the only argument when it is present'
    test:is(err, exp_err,
        '"insert" argument is forbidden with other filters (extra arguments)')

    -- test inserting an object into a collection with subrecord, union, array
    -- and map
    local mutation_insert_6 = [[
        mutation insert_order_metainfo {
            order_metainfo_collection(insert: {
                metainfo: "order metainfo 4000"
                order_metainfo_id: "order_metainfo_id_4000"
                order_metainfo_id_copy: "order_metainfo_id_4000"
                order_id: "order_id_4000"
                store: {
                    name: "store 4000"
                    address: {
                        street: "street 4000"
                        city: "city 4000"
                        state: "state 4000"
                        zip: "zip 4000"
                    }
                    second_address: {
                        street: "second street 4000"
                        city: "second city 4000"
                        state: "second state 4000"
                        zip: "second zip 4000"
                    }
                    external_id: {string: "eid_4000"}
                    tags: ["slow"]
                    parametrized_tags: {
                        size: "small"
                    }
                }
            }) {
                metainfo
                order_metainfo_id
                order_metainfo_id_copy
                order_id
                store {
                    name
                    address {
                        city
                    }
                    second_address {
                        city
                    }
                    external_id {
                        ... on String_box {
                            string
                        }
                        ... on Int_box {
                            int
                        }
                    }
                    tags
                    parametrized_tags
                }
            }
        }
    ]]

    local exp_result_insert_6 = yaml.decode(([[
        ---
        order_metainfo_collection:
        - metainfo: order metainfo 4000
          order_metainfo_id: order_metainfo_id_4000
          order_metainfo_id_copy: order_metainfo_id_4000
          order_id: order_id_4000
          store:
            name: store 4000
            address:
              city: city 4000
            second_address:
              city: second city 4000
            external_id:
              string: eid_4000
            tags:
            - slow
            parametrized_tags:
              size: small
    ]]):strip())

    check_insert_order_metainfo(test:test(
        'insert an object with subrecords (immediate argument)'),
        gql_wrapper, virtbox, mutation_insert_6, exp_result_insert_6)

    -- the same with a variable instead of immediate argument
    local mutation_insert_6v = [[
        mutation insert_order_metainfo(
            $order_metainfo: order_metainfo_collection_insert
        ) {
            order_metainfo_collection(insert: $order_metainfo) {
                metainfo
                order_metainfo_id
                order_metainfo_id_copy
                order_id
                store {
                    name
                    address {
                        city
                    }
                    second_address {
                        city
                    }
                    external_id {
                        ... on String_box {
                            string
                        }
                        ... on Int_box {
                            int
                        }
                    }
                    tags
                    parametrized_tags
                }
            }
        }
    ]]

    check_insert_order_metainfo(test:test(
        'insert an object with subrecords (variable argument)'),
        gql_wrapper, virtbox, mutation_insert_6v, exp_result_insert_6, {
            variables = {
                order_metainfo = {
                    metainfo = 'order metainfo 4000',
                    order_metainfo_id = 'order_metainfo_id_4000',
                    order_metainfo_id_copy = 'order_metainfo_id_4000',
                    order_id = 'order_id_4000',
                    store = {
                        name = 'store 4000',
                        address = {
                            street = 'street 4000',
                            city = 'city 4000',
                            state = 'state 4000',
                            zip = 'zip 4000',
                        },
                        second_address = {
                            street = 'second street 4000',
                            city = 'second city 4000',
                            state = 'second state 4000',
                            zip = 'second zip 4000',
                        },
                        external_id = {string = 'eid_4000'},
                        tags = {'slow'},
                        parametrized_tags = {
                            size = 'small',
                        }
                    }
                }
            }
        }
    )

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
    local exp_err = 'Non-existent argument "update"'
    test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
        '"update" argument is forbidden in a query')

    -- test updating of a field by which a shard key is calculated (it is the
    -- first field: tuple[1]);
    -- here we also check updating inside subrecords;
    -- and also check updating of array, map and union
    local mutation_update_subrecord = [[
        mutation update_order_metainfo {
            order_metainfo_collection(
                order_metainfo_id: "order_metainfo_id_1"
                update: {
                    metainfo: "changed"
                    store: {
                        address: {
                            city: "changed city"
                        }
                        second_address: {
                            city: "second changed city"
                        }
                        external_id: {string: "eid changed"}
                        tags: ["slow"]
                        parametrized_tags: {
                            size: "small"
                        }
                    }
                }
            ) {
                metainfo
                order_metainfo_id
                order_id
                store {
                    address {
                        city
                    }
                    second_address {
                        city
                    }
                    external_id {
                        ... on String_box {
                            string
                        }
                        ... on Int_box {
                            int
                        }
                    }
                    tags
                    parametrized_tags
                }
            }
        }
    ]]

    local exp_result_update_subrecord = yaml.decode(([[
        ---
        order_metainfo_collection:
        - metainfo: changed
          order_metainfo_id: order_metainfo_id_1
          order_id: order_id_1
          store:
            address:
              city: changed city
            second_address:
              city: second changed city
            external_id:
              string: eid changed
            tags:
            - slow
            parametrized_tags:
              size: small
    ]]):strip())

    check_update_order_metainfo(
        test:test('update 1st tuple field (immediate argument)'), gql_wrapper,
        virtbox, mutation_update_subrecord, exp_result_update_subrecord)

    -- the same with a variable argument
    local mutation_update_subrecord_v = [[
        mutation update_order_metainfo(
            $xorder_metainfo: order_metainfo_collection_update
        ) {
            order_metainfo_collection(
                order_metainfo_id: "order_metainfo_id_1"
                update: $xorder_metainfo
            ) {
                metainfo
                order_metainfo_id
                order_id
                store {
                    address {
                        city
                    }
                    second_address {
                        city
                    }
                    external_id {
                        ... on String_box {
                            string
                        }
                        ... on Int_box {
                            int
                        }
                    }
                    tags
                    parametrized_tags
                }
            }
        }
    ]]

    check_update_order_metainfo(
        test:test('update 1st tuple field (variable argument)'), gql_wrapper,
        virtbox, mutation_update_subrecord_v, exp_result_update_subrecord, {
            variables = {
                xorder_metainfo = {
                    metainfo = 'changed',
                    store = {
                        address = {
                            city = 'changed city',
                        },
                        second_address = {
                            city = 'second changed city',
                        },
                        external_id = {string = 'eid changed'},
                        tags = {'slow'},
                        parametrized_tags = {
                            size = 'small',
                        }
                    }
                }
            }
        }
    )

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
    local result = gql_mutation_update_4:execute(variables_update_4)
    local err = result.errors[1].message
    local exp_err = 'Unknown field "user_id" of the variable "xuser" ' ..
        'for the InputObject "user_collection_update"'
    test:is(err, exp_err,
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
    local result = gql_mutation_update_5:execute(variables_update_5)
    local err = result.errors[1].message
    local exp_err = 'Unknown field "order_id" of the variable "xorder" ' ..
        'for the InputObject "order_collection_update"'
    test:is(err, exp_err,
        'updating of a field of a primary key when it is shard key field')

    -- violation of an unique index constraint
    local mutation_update_6 = [[
        mutation {
            order_metainfo_collection(
                order_metainfo_id: "order_metainfo_id_1"
                update: {
                    metainfo: "updated"
                    order_metainfo_id_copy: "order_metainfo_id_2"
                }
            ) {
                metainfo
                order_metainfo_id
                order_metainfo_id_copy
                order_id
                store {
                    name
                }
            }
        }
    ]]
    local conf_name = test_run and test_run:get_cfg('conf') or 'space'
    if conf_name:startswith('shard') then
        local old_shard_key_hash = test_utils.get_shard_key_hash(
            'order_metainfo_id_1')
        local new_shard_key_hash = test_utils.get_shard_key_hash(
            'order_metainfo_id_2')
        -- check the case is really involving moving a tuple from one storage
        -- to an another
        assert(old_shard_key_hash ~= new_shard_key_hash)
    end
    local gql_mutation_update_6 = gql_wrapper:compile(mutation_update_6)
    test:test('unique constraint violation', function(test)
        test_utils.show_trace(function()
            test:plan(4)
            local order_metainfo_id = 'order_metainfo_id_1'

            -- save the original tuple
            local orig_tuple = get_tuple(virtbox, 'order_metainfo_collection',
                {order_metainfo_id})

            -- check mutation result from graphql
            local result = gql_mutation_update_6:execute({})
            local exp_err = "Duplicate key exists in unique index " ..
                "'order_metainfo_id_copy_index' in space " ..
                "'order_metainfo_collection'"
            local err = extract_storage_error(result.errors[1].message)
            test:is(err, exp_err, 'update result')

            -- check the user was not changed
            local tuple = get_tuple(virtbox, 'order_metainfo_collection',
                {order_metainfo_id})
            test:ok(tuple ~= nil, 'updated tuple exists')
            test:is_deeply((tuple or box.tuple.new({})):totable(),
                orig_tuple:totable(), 'tuple was not changed')

            -- replace back the tuple & check (in case the test fails and it
            -- was updated)
            replace_tuple(virtbox, 'order_metainfo_collection',
                {order_metainfo_id}, orig_tuple)
            local tuple = get_tuple(virtbox, 'order_metainfo_collection',
                {order_metainfo_id})
            test:is_deeply(tuple:totable(), orig_tuple:totable(),
                'tuple was replaced back')

            -- test:check() will be called automatically by the tap module
        end)
    end)

    -- }}}

    -- {{{ delete

    -- two deletes on top-level
    local mutation_delete_1 = [[
        mutation delete_user_and_order(
            $user_id: String,
            $order_id: String,
        ) {
            user_collection(user_id: $user_id, delete: true) {
                user_id
                first_name
                last_name
            }
            order_collection(order_id: $order_id, delete: true) {
                order_id
                description
                in_stock
            }
        }
    ]]

    local exp_result_delete_1 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          first_name: Ivan
          last_name: Ivanov
        order_collection:
        - order_id: order_id_1
          description: first order of Ivan
          in_stock: true
    ]]):strip())

    check_delete(test:test('delete_1'), gql_wrapper, virtbox, mutation_delete_1,
        exp_result_delete_1)

    -- two nested dependent deletes
    local mutation_delete_2 = [[
        mutation delete_user_and_order {
            user_collection(user_id: "user_id_1") {
                user_id
                first_name
                last_name
                order_connection(limit: 1, delete: true) {
                    order_id
                    description
                }
            }
            order_collection(order_id: "order_id_2") {
                order_id
                description
                in_stock
                user_connection(delete: true) {
                    user_id
                    first_name
                    last_name
                    order_connection(limit: 1) {
                        order_id
                        description
                    }
                }
            }
        }
    ]]

    local exp_result_delete_2 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_1
          first_name: Ivan
          last_name: Ivanov
          order_connection:
            - order_id: order_id_1
              description: first order of Ivan
        order_collection:
        - order_id: order_id_2
          description: second order of Ivan
          in_stock: false
          user_connection:
            user_id: user_id_1
            first_name: Ivan
            last_name: Ivanov
            order_connection:
              - order_id: order_id_2
                description: second order of Ivan
    ]]):strip())

    check_delete(test:test('delete_2'), gql_wrapper, virtbox, mutation_delete_2,
        exp_result_delete_2, {dont_pass_variables = true})

    -- test "delete" argument is forbidden in a query
    local query_delete = [[
        query delete_user_and_order(
            $user_id: String,
            $order_id: String,
        ) {
            user_collection(user_id: $user_id, delete: true) {
                user_id
                first_name
                last_name
            }
            order_collection(order_id: $order_id, delete: true) {
                order_id
                description
                in_stock
            }
        }
    ]]
    local ok, err = pcall(gql_wrapper.compile, gql_wrapper, query_delete)
    local exp_err = 'Non-existent argument "delete"'
    test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
        '"delete" argument is forbidden in a query')

    -- }}}

    assert(test:check(), 'check plan')
end

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()

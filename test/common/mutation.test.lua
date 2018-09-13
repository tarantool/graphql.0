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

box.cfg({})

local USER_SF = {1827767717}

-- replace back tuples & check
local function replace_user_and_order_back(test, virtbox, user_id, order_id,
        orig_user_object, orig_order_object)
    virtbox.user_collection:replace_object(orig_user_object, USER_SF)
    local object = virtbox.user_collection:get_object({user_id})
    test:is_deeply(object, orig_user_object, 'user object was replaced back')
    virtbox.order_collection:replace_object(orig_order_object)
    local object = virtbox.order_collection:get_object({order_id})
    test:is_deeply(object, orig_order_object, 'order object was replaced back')
end

local function check_insert(test, gql_wrapper, virtbox, mutation_insert,
        exp_result_insert, opts)
    local opts = opts or {}
    local dont_pass_variables = opts.dont_pass_variables or false

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
        test:is_deeply(result.data, exp_result_insert, 'insert result (check)')
        -- check inserted user
        local object = virtbox.user_collection:get_object({user_id})
        test:ok(object ~= nil, 'object was inserted')
        -- Todo: copy user input (variables) to preven side effects.
        object.bucket_id = variables_insert.user.bucket_id -- vshard
        test:is_deeply(object, variables_insert.user, 'inserted user object is correct')
        -- check inserted order
        local object = virtbox.order_collection:get_object({order_id})
        test:ok(object ~= nil, 'object was inserted')
        object.bucket_id = variables_insert.order.bucket_id -- vshard
        -- Fill in default value by hand.
        variables_insert.order.in_stock = true
        test:is_deeply(object, variables_insert.order, 'inserted order object is correct')
        -- clean up inserted tuples & check
        virtbox.user_collection:delete({user_id})
        local tuple = virtbox.user_collection:get({user_id})
        test:ok(tuple == nil, 'tuple was deleted')
        virtbox.order_collection:delete({order_id})
        local tuple = virtbox.order_collection:get({order_id})
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
        local tuple = virtbox.order_metainfo_collection:get({order_metainfo_id})
        test:ok(tuple == nil, 'tuple was not inserted before')

        -- check mutation result
        local gql_mutation_insert = gql_wrapper:compile(mutation_insert)
        local result = gql_mutation_insert:execute(variables)
        test:is_deeply(result.data, exp_result_insert, 'insert result')

        -- check inserted object
        local object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})
        test:ok(object ~= nil, 'inserted tuple exists')
        local exp_object = yaml.decode([[
            order_metainfo_id: order_metainfo_id_4000
            order_metainfo_id_copy: order_metainfo_id_4000
            metainfo: order metainfo 4000
            order_id: order_id_4000
            store:
              address:
                state: state 4000
                zip: zip 4000
                city: city 4000
                street: street 4000
              second_address:
                state: second state 4000
                zip: second zip 4000
                city: second city 4000
                street: second street 4000
              tags:
              - slow
              external_id:
                string: eid_4000
              name: store 4000
              parametrized_tags:
                size: small
        ]])
        object.bucket_id = nil -- vshard
        test:is_deeply(object, exp_object, 'inserted tuple is correct')

        -- delete inserted tuple & check
        virtbox.order_metainfo_collection:delete({order_metainfo_id})
        local tuple = virtbox.order_metainfo_collection:get({order_metainfo_id})
        test:ok(tuple == nil, 'inserted tuple was deleted')
        assert(test:check(), 'check plan')
    end)
end

local function check_update(test, gql_wrapper, virtbox, mutation_update,
        exp_result_update, opts)
    local opts = opts or {}
    local dont_pass_variables = opts.dont_pass_variables or false
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
        local orig_user_object = virtbox.user_collection:get_object({user_id})
        local orig_order_object = virtbox.order_collection:get_object({order_id})

        local gql_mutation_update = gql_wrapper:compile(mutation_update)

        -- check mutation result from graphql
        local result = gql_mutation_update:execute(dont_pass_variables and {} or
            variables_update)
        test:is_deeply(result.data, exp_result_update, 'update result')
        -- check updated user
        local object = virtbox.user_collection:get_object({user_id})
        test:ok(object ~= nil, 'updated object exists')
        local exp_object = table.copy(variables_update.xuser)
        for k, v in pairs(orig_user_object) do
            if exp_object[k] == nil then
                exp_object[k] = v
            end
        end
        object.bucket_id = exp_object.bucket_id -- vshard
        test:is_deeply(object, exp_object, 'updated object is correct')

        -- check updated order
        local object = virtbox.order_collection:get_object({order_id})
        test:ok(object ~= nil, 'updated object exists')
        local exp_object = table.copy(variables_update.xorder)
        for k, v in pairs(orig_order_object) do
            if exp_object[k] == nil then
                exp_object[k] = v
            end
        end
        object.bucket_id = exp_object.bucket_id -- vshard
        test:is_deeply(object, exp_object, 'updated object is correct')

        -- replace back updated tuples & check
        replace_user_and_order_back(test, virtbox, user_id, order_id,
            orig_user_object, orig_order_object)

        assert(test:check(), 'check plan')
    end)
end

local function check_update_order_metainfo(test, gql_wrapper, virtbox,
        mutation_update, exp_result_update, opts)
    local opts = opts or {}
    local variables = opts.variables or {}

    test_utils.show_trace(function()
        test:plan(5)

        -- check the original object
        local order_metainfo_id = 'order_metainfo_id_1'
        local orig_object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})
        local exp_orig_object = yaml.decode([[
            order_metainfo_id: order_metainfo_id_1
            order_metainfo_id_copy: order_metainfo_id_1
            metainfo: order metainfo 1
            order_id: order_id_1
            store:
              address:
                state: state 1
                zip: zip 1
                city: city 1
                street: street 1
              second_address:
                state: second state 1
                zip: second zip 1
                city: second city 1
                street: second street 1
              tags:
              - fast
              - new
              external_id:
                int: 1
              name: store 1
              parametrized_tags:
                size: medium
                since: '2018-01-01'
        ]])
        orig_object.bucket_id = nil -- vshard
        test:is_deeply(orig_object, exp_orig_object,
            'original object is the one that expected')

        -- check mutation result
        local gql_mutation_update = gql_wrapper:compile(mutation_update)
        local result = gql_mutation_update:execute(variables)
        test:is_deeply(result.data, exp_result_update, 'update result')

        -- check updated tuple
        local object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})
        test:ok(object ~= nil, 'updated object exists')
        object.bucket_id = nil
        local exp_object = table.copy(exp_orig_object)
        exp_object.metainfo = 'changed'
        exp_object.store.address.city = 'changed city'
        exp_object.store.second_address.city = 'second changed city'
        exp_object.store.external_id = {string = 'eid changed'}
        exp_object.store.tags = {'slow'}
        exp_object.store.parametrized_tags = {size = 'small'}
        test:is_deeply(object, exp_object, 'updated object is correct')

        -- replace back updated tuples & check
        virtbox.order_metainfo_collection:replace_object(orig_object)
        local object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})
        object.bucket_id = nil -- vshard
        test:is_deeply(object, orig_object, 'updated tuple was replaced back')

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
        local orig_user_object = virtbox.user_collection:get_object({user_id})
        local orig_order_object = virtbox.order_collection:get_object({order_id})

        local gql_mutation_delete = gql_wrapper:compile(mutation_delete)

        -- check mutation result from graphql
        local result = gql_mutation_delete:execute(dont_pass_variables and {} or
            variables_delete)
        test:is_deeply(result.data, exp_result_delete, 'delete result')

        -- check the user was deleted
        local tuple = virtbox.user_collection:get({user_id})
        test:ok(tuple == nil, 'tuple was deleted')

        -- check the order was deleted
        local tuple = virtbox.order_collection:get({order_id})
        test:ok(tuple == nil, 'tuple was deleted')

        -- replace back deleted tuples & check
        replace_user_and_order_back(test, virtbox, user_id, order_id,
            orig_user_object, orig_order_object)

        assert(test:check(), 'check plan')
    end)
end

local function extract_storage_error(err)
    return err:gsub('^failed to execute operation on[^:]+: *', '')
end

local function run_queries(gql_wrapper, virtbox)
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
        exp_result_insert_1)

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
        mutation_insert_1i, exp_result_insert_1, {dont_pass_variables = true})

    -- test non-top level "insert"
    local mutation_insert_2 = [[
        mutation insert_user_and_order($user: user_collection_insert,
                $order: order_collection_insert) {
            user_collection(insert: $user) {
                user_id
                first_name
                last_name
                order_connection(insert: $order) {
                    order_id
                    description
                    in_stock
                }
            }
        }
    ]]
    local exp_result_insert_2 = yaml.decode(([[
        ---
        user_collection:
        - user_id: user_id_new_1
          first_name: Peter
          last_name: Petrov
          order_connection:
          - order_id: order_id_new_1
            description: Peter's order
            in_stock: true
    ]]):strip())

    check_insert(test:test('insert_2-non-top-level'), gql_wrapper, virtbox, mutation_insert_2,
        exp_result_insert_2)

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
        - order_metainfo_id: order_metainfo_id_4000
          metainfo: order metainfo 4000
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
        exp_result_update_1)

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
        mutation_update_1i, exp_result_update_1, {dont_pass_variables = true})

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
        exp_result_update_2)

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
        mutation_update_2r, exp_result_update_2r)

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
        exp_result_update_3, {extra_xorder = {user_id = 'user_id_2'}})

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

    -- Orders #1 and #3 are on the same replicasets in cases of:
    -- vshard, shardX2 and shardX4.
    -- violation of an unique index constraint
    local mutation_update_6 = [[
        mutation {
            order_metainfo_collection(
                order_metainfo_id: "order_metainfo_id_1"
                update: {
                    metainfo: "updated"
                    order_metainfo_id_copy: "order_metainfo_id_3"
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
    -- TODO: Check constraint error in case an object moves from one
    -- storage to another on update for `shard` accessor.
    local gql_mutation_update_6 = gql_wrapper:compile(mutation_update_6)
    test:test('unique constraint violation', function(test)
        test_utils.show_trace(function()
            test:plan(4)
            local order_metainfo_id = 'order_metainfo_id_1'

            -- save the original object
            local orig_object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})

            -- check mutation result from graphql
            local result = gql_mutation_update_6:execute({})
            local exp_err = "Duplicate key exists in unique index " ..
                "'order_metainfo_id_copy_index' in space " ..
                "'order_metainfo_collection'"
            local err = extract_storage_error(result.errors[1].message)
            test:is(err, exp_err, 'update result')

            -- check the data was not changed
            local object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})
            test:ok(object ~= nil, 'updated object exists')
            test:is_deeply(object, orig_object, 'object was not changed')

            -- replace back the object & check (in case the test fails and it
            -- was updated)
            virtbox.order_metainfo_collection:replace_object(orig_object)
            local object = virtbox.order_metainfo_collection:get_object({order_metainfo_id})
            object.bucket_id = orig_object.bucket_id --vshard
            test:is_deeply(object, orig_object, 'object was replaced back')

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

-- Mutations are disabled for avro-schema-2* by default, but can be enabled by
-- the option.
local function run_queries_avro_schema_2(test, enable_mutations, gql_wrapper,
        virtbox)
    local mutation_insert = [[
        mutation insert_user($user: user_collection_insert) {
            user_collection(insert: $user) {
                user_id
                first_name
                last_name
            }
        }
    ]]
    local ok, err = pcall(gql_wrapper.compile, gql_wrapper, mutation_insert)

    if enable_mutations then
        test:ok(ok, 'mutations are enabled with the enable_mutations flag')
    else
        local exp_err = 'Variable specifies unknown type ' ..
            '"user_collection_insert"'
        test:is_deeply({ok, utils.strip_error(err)}, {false, exp_err},
            'mutations are forbidden for avro-schema-2*')
    end
end

if test_utils.major_avro_schema_version() == 3 then
    test_utils.run_testdata(testdata, {
        run_queries = run_queries,
    })
else
    local test = tap.test('mutation')
    test:plan(2)
    local function workload(ctx, virtbox)
        testdata.fill_test_data(virtbox)
        -- test mutations are disabled on avro-schema-2* by default
        local gql_wrapper = test_utils.graphql_from_testdata(testdata, {}, ctx)
        run_queries_avro_schema_2(test, false, gql_wrapper, virtbox)
        -- test mutations can be enabled on avro-schema-2* by the option
        local gql_wrapper = test_utils.graphql_from_testdata(testdata,
            {enable_mutations = true}, ctx)
        run_queries_avro_schema_2(test, true, gql_wrapper, virtbox)
    end
    test_utils.run_testdata(testdata, {
        workload = workload,
    })
    assert(test:check(), 'check plan')
end

os.exit()

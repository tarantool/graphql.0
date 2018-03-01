local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

local compound_index_testdata = {}

-- return an error w/o file name and line number
local function strip_error(err)
    return tostring(err):gsub('^.-:.-: (.*)$', '%1')
end

local function print_and_return(...)
    print(...)
    return table.concat({...}, ' ') .. '\n'
end

local function format_result(name, query, variables, result)
    return ('RUN %s {{{\nQUERY\n%s\nVARIABLES\n%s\nRESULT\n%s\n}}}\n'):format(
        name, query:rstrip(), yaml.encode(variables), yaml.encode(result))
end

-- schemas and meta-information
-- ----------------------------

function compound_index_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                { "name": "user_str", "type": "string" },
                { "name": "user_num", "type": "long" },
                { "name": "first_name", "type": "string" },
                { "name": "last_name", "type": "string" }
            ]
        },
        "order": {
            "type": "record",
            "name": "order",
            "fields": [
                { "name": "order_str", "type": "string" },
                { "name": "order_num", "type": "long" },
                { "name": "user_str", "type": "string" },
                { "name": "user_num", "type": "long" },
                { "name": "description", "type": "string" }
            ]
        }
    }]])

    local collections = json.decode([[{
        "user_collection": {
            "schema_name": "user",
            "connections": [
                {
                    "type": "1:N",
                    "name": "order_connection",
                    "destination_collection": "order_collection",
                    "parts": [
                        { "source_field": "user_str", "destination_field": "user_str" },
                        { "source_field": "user_num", "destination_field": "user_num" }
                    ],
                    "index_name": "user_str_num_index"
                },
                {
                    "type": "1:N",
                    "name": "order_str_connection",
                    "destination_collection": "order_collection",
                    "parts": [
                        { "source_field": "user_str", "destination_field": "user_str" }
                    ],
                    "index_name": "user_str_num_index"
                }
            ]
        },
        "order_collection": {
            "schema_name": "order",
            "connections": [
                {
                    "type": "1:1",
                    "name": "user_connection",
                    "destination_collection":  "user_collection",
                    "parts": [
                        { "source_field": "user_str", "destination_field": "user_str" },
                        { "source_field": "user_num", "destination_field": "user_num" }
                    ],
                    "index_name": "user_str_num_index"
                }
            ]
        }
    }]])

    local service_fields = {
        user = {},
        order = {},
    }

    local indexes = {
        user_collection = {
            user_str_num_index = {
                service_fields = {},
                fields = {'user_str', 'user_num'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
        order_collection = {
            order_str_num_index = {
                service_fields = {},
                fields = {'order_str', 'order_num'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            user_str_num_index = {
                service_fields = {},
                fields = {'user_str', 'user_num'},
                index_type = 'tree',
                unique = false,
                primary = false,
            },
        },
    }

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function compound_index_testdata.init_spaces()
    -- user_collection fields
    local U_USER_STR_FN = 1
    local U_USER_NUM_FN = 2

    -- order_collection fields
    local O_ORDER_STR_FN = 1
    local O_ORDER_NUM_FN = 2
    local O_USER_STR_FN = 3
    local O_USER_NUM_FN = 4

    box.once('test_space_init_spaces', function()
        -- users
        box.schema.create_space('user_collection')
        box.space.user_collection:create_index('user_str_num_index',
            {type = 'tree', unique = true, parts = {
                U_USER_STR_FN, 'string', U_USER_NUM_FN, 'unsigned',
            }}
        )

        -- orders
        box.schema.create_space('order_collection')
        box.space.order_collection:create_index('order_str_num_index',
            {type = 'tree', unique = true, parts = {
                O_ORDER_STR_FN, 'string', O_ORDER_NUM_FN, 'unsigned',
            }}
        )
        box.space.order_collection:create_index('user_str_num_index',
            {type = 'tree', unique = false, parts = {
                O_USER_STR_FN, 'string', O_USER_NUM_FN, 'unsigned',
            }}
        )
    end)
end

function compound_index_testdata.fill_test_data(shard)
    local shard = shard or box.space

    for i = 1, 20 do
        for j = 1, 5 do
            local s =
                j % 5 == 1 and 'a' or
                j % 5 == 2 and 'b' or
                j % 5 == 3 and 'c' or
                j % 5 == 4 and 'd' or
                j % 5 == 0 and 'e' or
                nil
            assert(s ~= nil, 's must not be nil')
            local user_str = 'user_str_' .. s
            local user_num = i
            shard.user_collection:replace(
                {user_str, user_num, 'first name ' .. s, 'last name ' .. s})
            for k = 1, 10 do
                local order_str = 'order_str_' .. s .. '_' .. tostring(k)
                local order_num = i * 100 + k
                shard.order_collection:replace(
                    {order_str, order_num, user_str, user_num,
                    'description ' .. s})
            end
        end
    end
end

function compound_index_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.user_collection:drop()
    box.space.order_collection:drop()
end

function compound_index_testdata.run_queries(gql_wrapper)
    local results = ''

    -- get a top-level object by a full compound primary key
    -- -----------------------------------------------------

    local query_1 = [[
        query users($user_str: String, $user_num: Long,
                $first_name: String) {
            user_collection(user_str: $user_str, user_num: $user_num
                    first_name: $first_name) {
                user_str
                user_num
                last_name
                first_name
            }
        }
    ]]

    local gql_query_1 = gql_wrapper:compile(query_1)

    utils.show_trace(function()
        local variables_1_1 = {user_str = 'user_str_b', user_num = 12}
        local result = gql_query_1:execute(variables_1_1)
        results = results .. print_and_return(format_result(
            '1_1', query_1, variables_1_1, result))
    end)

    -- get a top-level object by a full compound primary key plus filter
    -- -----------------------------------------------------------------

    utils.show_trace(function()
        local variables_1_2 = {
            user_str = 'user_str_b',
            user_num = 12,
            first_name = 'non-existent',
        }
        local result = gql_query_1:execute(variables_1_2)
        results = results .. print_and_return(format_result(
            '1_2', query_1, variables_1_2, result))
    end)

    -- select top-level objects by a partial compound primary key (or maybe use
    -- fullscan)
    -- ------------------------------------------------------------------------

    utils.show_trace(function()
        local variables_1_3 = {user_num = 12}
        local result = gql_query_1:execute(variables_1_3)
        results = results .. print_and_return(format_result(
            '1_3', query_1, variables_1_3, result))
    end)

    utils.show_trace(function()
        local variables_1_4 = {user_str = 'user_str_b'}
        local result = gql_query_1:execute(variables_1_4)
        results = results .. print_and_return(format_result(
            '1_4', query_1, variables_1_4, result))
    end)

    -- select top-level objects by a partial compound primary key plus filter
    -- (or maybe use fullscan)
    -- ----------------------------------------------------------------------

    utils.show_trace(function()
        local variables_1_5 = {
            user_num = 12,
            first_name = 'non-existent'
        }
        local result = gql_query_1:execute(variables_1_5)
        results = results .. print_and_return(format_result(
            '1_5', query_1, variables_1_5, result))
    end)

    utils.show_trace(function()
        local variables_1_6 = {
            user_str = 'user_str_b',
            first_name = 'non-existent'
        }
        local result = gql_query_1:execute(variables_1_6)
        results = results .. print_and_return(format_result(
            '1_6', query_1, variables_1_6, result))
    end)

    -- select objects by a connection by a full compound index
    -- -------------------------------------------------------

    local query_2 = [[
        query users($user_str: String, $user_num: Long, $description: String) {
            user_collection(user_str: $user_str, user_num: $user_num) {
                user_str
                user_num
                last_name
                first_name
                order_connection(description: $description) {
                    order_str
                    order_num
                    description
                }
            }
        }
    ]]

    local gql_query_2 = gql_wrapper:compile(query_2)

    utils.show_trace(function()
        local variables_2_1 = {user_str = 'user_str_b', user_num = 12}
        local result = gql_query_2:execute(variables_2_1)
        results = results .. print_and_return(format_result(
            '2_1', query_2, variables_2_1, result))
    end)

    -- select objects by a connection by a full compound index plus filter
    -- -------------------------------------------------------------------

    utils.show_trace(function()
        local variables_2_2 = {
            user_str = 'user_str_b',
            user_num = 12,
            description = 'non-existent',
        }
        local result = gql_query_2:execute(variables_2_2)
        results = results .. print_and_return(format_result(
            '2_2', query_2, variables_2_2, result))
    end)

    -- select object by a connection by a partial compound index
    -- ---------------------------------------------------------

    local query_3 = [[
        query users($user_str: String, $user_num: Long) {
            user_collection(user_str: $user_str, user_num: $user_num) {
                user_str
                user_num
                last_name
                first_name
                order_str_connection {
                    order_str
                    order_num
                    description
                }
            }
        }
    ]]

    utils.show_trace(function()
        local gql_query_3 = gql_wrapper:compile(query_3)
        local variables_3 = {user_str = 'user_str_b', user_num = 12}
        local result = gql_query_3:execute(variables_3)
        results = results .. print_and_return(format_result(
            '3', query_3, variables_3, result))
    end)

    -- offset on top-level by a full compound primary key
    -- --------------------------------------------------

    local query_4 = [[
        query users($limit: Int, $offset: user_collection_offset) {
            user_collection(limit: $limit, offset: $offset) {
                user_str
                user_num
                last_name
                first_name
            }
        }
    ]]

    local gql_query_4 = gql_wrapper:compile(query_4)

    utils.show_trace(function()
        local variables_4_1 = {
            limit = 10,
            offset = {
                user_str = 'user_str_b',
                user_num = 12,
            }
        }
        local result = gql_query_4:execute(variables_4_1)
        results = results .. print_and_return(format_result(
            '4_1', query_4, variables_4_1, result))
    end)

    -- offset on top-level by a partial compound primary key (expected to fail)
    -- ------------------------------------------------------------------------

    local variables_4_2 = {
        limit = 10,
        offset = {
            user_str = 'user_str_b',
        }
    }
    local ok, err = pcall(function()
        local result = gql_query_4:execute(variables_4_2)
        results = results .. print_and_return(format_result(
            '4_2', query_4, variables_4_2, result))
    end)

    local result = {ok = ok, err = strip_error(err)}
    results = results .. print_and_return(format_result(
        '4_2', query_4, variables_4_2, result))

    -- offset when using a connection by a full compound primary key
    -- -------------------------------------------------------------

    local query_5 = [[
        query users($user_str: String, $user_num: Long,
                $limit: Int, $offset: order_collection_offset) {
            user_collection(user_str: $user_str, user_num: $user_num) {
                user_str
                user_num
                last_name
                first_name
                order_connection(limit: $limit, offset: $offset) {
                    order_str
                    order_num
                    description
                }
            }
        }
    ]]

    local gql_query_5 = gql_wrapper:compile(query_5)

    utils.show_trace(function()
        local variables_5_1 = {
            user_str = 'user_str_b',
            user_num = 12,
            limit = 4,
            offset = {
                order_str = 'order_str_b_2',
                order_num = 1202,
            }
        }
        local result = gql_query_5:execute(variables_5_1)
        results = results .. print_and_return(format_result(
            '5_1', query_5, variables_5_1, result))
    end)

    -- offset when using a connection by a partial compound primary key
    -- (expected to fail)
    -- ----------------------------------------------------------------

    local variables_5_2 = {
        user_str = 'user_str_b',
        user_num = 12,
        limit = 4,
        offset = {
            order_str = 'order_str_b_2',
        }
    }
    local ok, err = pcall(function()
        local result = gql_query_5:execute(variables_5_2)
        results = results .. print_and_return(format_result(
            '5_2', query_5, variables_5_2, result))
    end)

    local result = {ok = ok, err = strip_error(err)}
    results = results .. print_and_return(format_result(
        '5_2', query_5, variables_5_2, result))

    return results
end

return compound_index_testdata

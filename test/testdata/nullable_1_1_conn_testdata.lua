-- The example was inspired by [1]. Consider [2] for the problem description
-- and [3] for the track of related task.
--
-- [1]: https://www.jwz.org/doc/mid.html
-- [2]: https://github.com/tarantool/graphql/issues/43
-- [3]: https://github.com/tarantool/graphql/issues/44

local json = require('json')
local yaml = require('yaml')
local utils = require('graphql.utils')

local nullable_1_1_conn_testdata = {}

local PRNG_SEED = 42
local DOMAIN = 'graphql.tarantool.org'

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

function nullable_1_1_conn_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "email": {
            "type": "record",
            "name": "email",
            "fields": [
                { "name": "localpart", "type": "string" },
                { "name": "domain", "type": "string" },
                { "name": "in_reply_to_localpart", "type": "string*" },
                { "name": "in_reply_to_domain", "type": "string*" },
                { "name": "body", "type": "string" }
            ]
        }
    }]])

    local collections = json.decode([[{
        "email": {
            "schema_name": "email",
            "connections": [
                {
                    "type": "1:N",
                    "name": "successors",
                    "destination_collection": "email",
                    "parts": [
                        {
                            "source_field": "localpart",
                            "destination_field": "in_reply_to_localpart"
                        },
                        {
                            "source_field": "domain",
                            "destination_field": "in_reply_to_domain"
                        }
                    ],
                    "index_name": "in_reply_to"
                },
                {
                    "type": "1:1*",
                    "name": "in_reply_to",
                    "destination_collection": "email",
                    "parts": [
                        {
                            "source_field": "in_reply_to_localpart",
                            "destination_field": "localpart"
                        },
                        {
                            "source_field": "in_reply_to_domain",
                            "destination_field": "domain"
                        }
                    ],
                    "index_name": "message_id"
                }
            ]
        }
    }]])

    local service_fields = {
        email = {},
    }

    local indexes = {
        email = {
            message_id = {
                service_fields = {},
                fields = {'localpart', 'domain'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            in_reply_to = {
                service_fields = {},
                fields = {'in_reply_to_localpart', 'in_reply_to_domain'},
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

function nullable_1_1_conn_testdata.init_spaces()
    -- email fields
    local LOCALPART_FN = 1
    local DOMAIN_FN = 2
    local IN_REPLY_TO_LOCALPART_BRANCH_FN = 3 -- luacheck: ignore
    local IN_REPLY_TO_LOCALPART_FN = 4
    local IN_REPLY_TO_DOMAIN_BRANCH_FN = 5 -- luacheck: ignore
    local IN_REPLY_TO_DOMAIN_FN = 6
    local BODY_FN = 7 -- luacheck: ignore

    box.once('test_space_init_spaces', function()
        box.schema.create_space('email')
        box.space.email:create_index('message_id',
            {type = 'tree', unique = true, parts = {
                {LOCALPART_FN, 'string'},
                {DOMAIN_FN, 'string'},
            }}
        )
        box.space.email:create_index('in_reply_to',
            {type = 'tree', unique = false, parts = {
                {IN_REPLY_TO_LOCALPART_FN, 'string', is_nullable = true},
                {IN_REPLY_TO_DOMAIN_FN, 'string', is_nullable = true},
            }}
        )
    end)
end

-- numbers are from https://gist.github.com/blixt/f17b47c62508be59987b
local function gen_prng(seed)
    return setmetatable({seed = seed}, {
        __index = {
            next_int = function(self, min, max)
                self.seed = self.seed * 16807 % 2147483647
                return self.seed % (max - min + 1) + min
            end,
            next_string = function(self, len)
                local res = {}
                for i = 1, len do
                    res[i] = string.char(self:next_int(0, 255))
                end
                return table.concat(res)
            end,
        }
    })
end

function nullable_1_1_conn_testdata.fill_test_data(virtbox)
    local results = ''

    local virtbox = virtbox or box.space

    local prng = gen_prng(PRNG_SEED)

    local function new_email(body)
        return {
            localpart = prng:next_string(16):hex(),
            domain = DOMAIN,
            body = body,
        }
    end

    -- the string must contain '\n\n' to being printed in the literal scalar
    -- style
    results = results .. print_and_return(([[


        +---------------------+
        |  a-+      h     x y |
        |  |\ \     |\        |
        |  b c d    k l       |
        |  |   |\      \      |
        |  e   f g      m     |
        +---------------------+
    ]]):rstrip())

    local email_trees = {
        {
            email = new_email('a'),
            successors = {
                {
                    email = new_email('b'),
                    successors = {
                        {email = new_email('e')},
                    }
                },
                {
                    email = new_email('c'),
                },
                {
                    email = new_email('d'),
                    successors = {
                        {email = new_email('f')},
                        {email = new_email('g')},
                    }
                }
            }
        },
        {
            email = new_email('h'),
            successors = {
                {
                    email = new_email('k'),
                },
                {
                    email = new_email('l'),
                    successors = {
                        {email = new_email('m')}
                    }
                }
            },
        }
    }

    local function union_branch_of(value)
        local NULL_T = 0
        local STRING_T = 1

        if value == nil then
            return NULL_T
        elseif type(value) == 'string' then
            return STRING_T
        end
        error('value must be nil or a string, got ' .. type(value))
    end

    -- `in_reply_to` is optional parameter with the following format:
    --
    -- ```
    -- {
    --     localpart = '...',
    --     domain = '...',
    -- }
    -- ```
    local function add_emails(email_nodes, in_reply_to)
        local irt_localpart = (in_reply_to or {}).localpart
        local irt_domain = (in_reply_to or {}).domain

        for _, email_node in pairs(email_nodes) do
            local localpart = email_node.email.localpart
            local domain = email_node.email.domain

            virtbox.email:replace({
                localpart,
                domain,
                union_branch_of(irt_localpart),
                irt_localpart,
                union_branch_of(irt_domain),
                irt_domain,
                email_node.email.body,
            })
            add_emails(email_node.successors or {}, {
                localpart = localpart,
                domain = domain,
            })
        end
    end

    add_emails(email_trees)

    -- add two emails with one null in in_reply_to_{localpart,domain} to test
    -- FULL MATCH constraints
    local domain = DOMAIN
    local localpart = prng:next_string(16):hex()
    virtbox.email:replace({
        localpart,
        domain,
        union_branch_of(box.NULL),
        box.NULL,
        union_branch_of(domain),
        domain,
        'x',
    })
    local localpart = prng:next_string(16):hex()
    virtbox.email:replace({
        localpart,
        domain,
        union_branch_of(localpart),
        localpart,
        union_branch_of(box.NULL),
        box.NULL,
        'y',
    })

    return results
end

function nullable_1_1_conn_testdata.drop_spaces()
    box.space._schema:delete('oncetest_space_init_spaces')
    box.space.email:drop()
end

function nullable_1_1_conn_testdata.run_queries(gql_wrapper)
    local results = ''

    -- downside traversal (1:N connections)
    -- ------------------------------------

    local query_downside = [[
        query emails_tree_downside($body: String) {
            email(body: $body) {
                body
                successors {
                    body
                    successors {
                        body
                        successors {
                            body
                        }
                    }
                }
            }
        }
    ]]

    local gql_query_downside = gql_wrapper:compile(query_downside)

    utils.show_trace(function()
        local variables_downside_a = {body = 'a'}
        local result = gql_query_downside:execute(variables_downside_a)
        results = results .. print_and_return(format_result(
            'downside_a', query_downside, variables_downside_a, result))
    end)

    utils.show_trace(function()
        local variables_downside_h = {body = 'h'}
        local result = gql_query_downside:execute(variables_downside_h)
        results = results .. print_and_return(format_result(
            'downside_h', query_downside, variables_downside_h, result))
    end)

    -- upside traversal (1:1 connections)
    -- ----------------------------------

    local query_upside = [[
        query emails_trace_upside($body: String) {
            email(body: $body) {
                body
                in_reply_to {
                    body
                    in_reply_to {
                        body
                        in_reply_to {
                            body
                        }
                    }
                }
            }
        }
    ]]

    local gql_query_upside = gql_wrapper:compile(query_upside)

    utils.show_trace(function()
        local variables_upside = {body = 'f'}
        local result = gql_query_upside:execute(variables_upside)
        results = results .. print_and_return(format_result(
            'upside', query_upside, variables_upside, result))
    end)

    -- FULL MATCH constraint: connection key parts must be all non-nulls or all
    -- nulls; both expected to fail
    -- ------------------------------------------------------------------------

    local variables_upside_x = {body = 'x'}
    local ok, err = pcall(function()
        local result = gql_query_upside:execute(variables_upside_x)
        results = results .. print_and_return(format_result(
            'upside_x', query_upside, variables_upside_x, result))
    end)

    local result = {ok = ok, err = strip_error(err)}
    results = results .. print_and_return(format_result(
        'upside_x', query_upside, variables_upside_x, result))

    local variables_upside_y = {body = 'y'}
    local ok, err = pcall(function()
        local result = gql_query_upside:execute(variables_upside_y)
        results = results .. print_and_return(format_result(
            'upside_y', query_upside, variables_upside_y, result))
    end)

    local result = {ok = ok, err = strip_error(err)}
    results = results .. print_and_return(format_result(
        'upside_y', query_upside, variables_upside_y, result))

    return results
end

return nullable_1_1_conn_testdata

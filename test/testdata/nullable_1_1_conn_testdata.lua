-- The example was inspired by [1]. Consider [2] for the problem description
-- and [3] for the track of related task.
--
-- [1]: https://www.jwz.org/doc/mid.html
-- [2]: https://github.com/tarantool/graphql/issues/43
-- [3]: https://github.com/tarantool/graphql/issues/44

local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local test_utils = require('test.test_utils')

local nullable_1_1_conn_testdata = {}

local PRNG_SEED = 42
local DOMAIN = 'graphql.tarantool.org'

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
                    "type": "1:1",
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

function nullable_1_1_conn_testdata.init_spaces(avro_version)
    -- email fields
    local LOCALPART_FN = 1
    local DOMAIN_FN = 2
    local IN_REPLY_TO_LOCALPART_FN = avro_version == 3 and 3 or 4
    local IN_REPLY_TO_DOMAIN_FN = avro_version == 3 and 4 or 6

    box.once('init_spaces_nullable_1_1_conn', function()
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

--[[
        +---------------------+
        |  a-+      h     x y |
        |  |\ \     |\        |
        |  b c d    k l       |
        |  |   |\      \      |
        |  e   f g      m     |
        +---------------------+
]]--
function nullable_1_1_conn_testdata.fill_test_data(virtbox, meta)
    local virtbox = virtbox or box.space

    local prng = gen_prng(PRNG_SEED)

    local function new_email(body)
        return {
            localpart = prng:next_string(16):hex(),
            domain = DOMAIN,
            body = body,
        }
    end

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

            test_utils.replace_object(virtbox, meta, 'email', {
                localpart = localpart,
                domain = domain,
                in_reply_to_localpart = irt_localpart,
                in_reply_to_domain = irt_domain,
                body = email_node.email.body,
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
    test_utils.replace_object(virtbox, meta, 'email', {
        localpart = localpart,
        domain = domain,
        in_reply_to_localpart = box.NULL,
        in_reply_to_domain = domain,
        body = 'x',
    })
    local localpart = prng:next_string(16):hex()
    test_utils.replace_object(virtbox, meta, 'email', {
        localpart = localpart,
        domain = domain,
        in_reply_to_localpart = localpart,
        in_reply_to_domain = box.NULL,
        body = 'y',
    })

    -- Check dangling 1:1 connection.
    local localpart = prng:next_string(16):hex()
    local non_existent_localpart = prng:next_string(16):hex()
    test_utils.replace_object(virtbox, meta, 'email', {
        localpart = localpart,
        domain = domain,
        in_reply_to_localpart = non_existent_localpart,
        in_reply_to_domain = DOMAIN,
        body = 'z',
    })
end

function nullable_1_1_conn_testdata.drop_spaces()
    box.space._schema:delete('onceinit_spaces_nullable_1_1_conn')
    box.space.email:drop()
end

function nullable_1_1_conn_testdata.run_queries(gql_wrapper)
    local test = tap.test('nullable_1_1_conn')
    test:plan(7)

    -- {{{ downside traversal (1:N connections)

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

    local gql_query_downside = test_utils.show_trace(function()
        return gql_wrapper:compile(query_downside)
    end)

    local result = test_utils.show_trace(function()
        local variables_downside_a = {body = 'a'}
        return gql_query_downside:execute(variables_downside_a)
    end)

    local exp_result = yaml.decode(([[
        ---
        email:
        - successors:
          - successors: &0 []
            body: c
          - successors:
            - successors: *0
              body: g
            - successors: *0
              body: f
            body: d
          - successors:
            - successors: *0
              body: e
            body: b
          body: a
    ]]):strip())

    test:is_deeply(result.data, exp_result, 'downside_a')

    local result = test_utils.show_trace(function()
        local variables_downside_h = {body = 'h'}
        return gql_query_downside:execute(variables_downside_h)
    end)

    local exp_result = yaml.decode(([[
        ---
        email:
        - successors:
          - successors:
            - successors: &0 []
              body: m
            body: l
          - successors: *0
            body: k
          body: h
    ]]):strip())

    test:is_deeply(result.data, exp_result, 'downside_h')

    -- }}}
    -- {{{ upside traversal (1:1 connections)

    local query_upside = [[
        query emails_trace_upside($body: String, $child_domain: String) {
            email(body: $body) {
                body
                in_reply_to(domain: $child_domain) {
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

    local result = test_utils.show_trace(function()
        local variables_upside = {body = 'f'}
        return gql_query_upside:execute(variables_upside)
    end)

    local exp_result = yaml.decode(([[
        ---
        email:
        - body: f
          in_reply_to:
            body: d
            in_reply_to:
              body: a
    ]]):strip())

    test:is_deeply(result.data, exp_result, 'upside')

    -- }}}
    -- {{{ FULL MATCH constraint

    -- connection key parts must be all non-nulls or all nulls; both expected
    -- to fail

    local variables_upside_x = {body = 'x'}
    local result = gql_query_upside:execute(variables_upside_x)
    local err = result.errors[1].message
    local exp_err = 'FULL MATCH constraint was failed: connection key parts ' ..
        'must be all non-nulls or all nulls; object: ' ..
        '{"domain":"graphql.tarantool.org",' ..
        '"localpart":"062b56b1885c71c51153ccb880ac7315","body":"x",' ..
        '"in_reply_to_domain":"graphql.tarantool.org",' ..
        '"in_reply_to_localpart":null}'
    test:is(err, exp_err, 'upside_x')

    local variables_upside_y = {body = 'y'}
    local result = gql_query_upside:execute(variables_upside_y)
    local err = result.errors[1].message
    local exp_err = 'FULL MATCH constraint was failed: connection key parts ' ..
        'must be all non-nulls or all nulls; object: ' ..
        '{"domain":"graphql.tarantool.org",' ..
        '"localpart":"1f70391f6ba858129413bd801b12acbf","body":"y",' ..
        '"in_reply_to_domain":null,' ..
        '"in_reply_to_localpart":"1f70391f6ba858129413bd801b12acbf"}'
    test:is(err, exp_err, 'upside_y')

    -- Check we get an error when trying to use dangling 1:1 connection. Check
    -- we don't get this error when `disable_dangling_check` is set.
    if gql_wrapper.disable_dangling_check then
        local variables_upside_z = {body = 'z'}
        local result = test_utils.show_trace(function()
            return gql_query_upside:execute(variables_upside_z)
        end)

        local exp_result = yaml.decode(([[
            ---
            email:
            - body: z
        ]]):strip())

        test:is_deeply(result.data, exp_result, 'upside_z disabled constraint check')
    else
        local variables_upside_z = {body = 'z'}
        local result = gql_query_upside:execute(variables_upside_z)
        local err = result.errors[1].message
        local exp_err = 'FULL MATCH constraint was failed: we expect 1 ' ..
            'tuples, got 0'
        test:is(err, exp_err, 'upside_z constraint violation')
    end

    -- We can got zero objects by 1:1 connection when use filters, it is not
    -- violation of FULL MATCH constraint, because we found corresponding
    -- tuple, but filter it then.
    local variables_upside_f = {body = 'f', child_domain = 'non-existent'}
    local result = test_utils.show_trace(function()
        return gql_query_upside:execute(variables_upside_f)
    end)

    local exp_result = yaml.decode(([[
        ---
        email:
        - body: f
    ]]):strip())

    test:is_deeply(result.data, exp_result, 'upside_f filter child')

    assert(test:check(), 'check plan')
end

return nullable_1_1_conn_testdata
